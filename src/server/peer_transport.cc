#include "server/peer_transport.h"

#include <chrono>

#include "net/client.h"
#include "net/frame.h"

namespace kv::server {

PeerTransport::PeerTransport(raft::NodeId self, const std::map<raft::NodeId, std::string>& peers) {
  for (const auto& [id, addr] : peers) {
    if (id == self) {
      continue;
    }
    auto p = std::make_unique<Peer>();
    p->addr = addr;
    peers_[id] = std::move(p);
  }
  for (auto& [id, p] : peers_) {
    p->thread = std::thread([this, peer = p.get()] { Run(peer); });
  }
}

PeerTransport::~PeerTransport() {
  stopping_ = true;
  for (auto& [id, p] : peers_) {
    {
      std::lock_guard lk(p->mu);
    }
    p->cv.notify_all();
  }
  for (auto& [id, p] : peers_) {
    p->thread.join();
  }
}

void PeerTransport::Send(const raft::Message& m) {
  const auto it = peers_.find(m.to);
  if (it == peers_.end()) {
    return;
  }
  net::Frame frame;
  frame.type = net::MsgType::kRaft;
  frame.payload = raft::EncodeMessage(m);
  std::string bytes = net::EncodeFrame(frame);
  Peer& p = *it->second;
  {
    std::lock_guard lk(p.mu);
    if (p.queue.size() >= kMaxQueue) {
      p.queue.pop_front();
      ++p.dropped;
    }
    p.queue.push_back(std::move(bytes));
  }
  p.cv.notify_one();
}

uint64_t PeerTransport::dropped() const {
  uint64_t total = 0;
  for (const auto& [id, p] : peers_) {
    std::lock_guard lk(p->mu);
    total += p->dropped;
  }
  return total;
}

void PeerTransport::Run(Peer* peer) {
  using std::chrono::milliseconds;
  std::unique_ptr<net::Connection> conn;
  auto backoff = milliseconds(20);
  while (!stopping_) {
    std::deque<std::string> batch;
    {
      std::unique_lock lk(peer->mu);
      peer->cv.wait(lk, [&] { return stopping_ || !peer->queue.empty(); });
      if (stopping_) {
        return;
      }
      batch.swap(peer->queue);
    }
    if (!conn) {
      if (!net::Connection::Dial(peer->addr, milliseconds(500), &conn).ok()) {
        // Peer unreachable. Drop this batch rather than letting it go stale; Raft
        // resends what matters. Back off so a dead peer costs little CPU.
        std::unique_lock lk(peer->mu);
        peer->dropped += batch.size();
        peer->cv.wait_for(lk, backoff, [&] { return stopping_.load(); });
        backoff = std::min(backoff * 2, milliseconds(1000));
        continue;
      }
      backoff = milliseconds(20);
    }
    std::string buf;
    for (const auto& b : batch) {
      buf.append(b);
    }
    if (!conn->SendBytes(buf, milliseconds(1000)).ok()) {
      conn.reset();
    }
  }
}

}  // namespace kv::server

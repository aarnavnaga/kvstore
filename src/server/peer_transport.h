#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "net/tls.h"
#include "raft/types.h"

namespace kv::server {

// PeerTransport delivers Raft messages to the other nodes. Each peer gets its own queue
// and sender thread, so a slow or dead peer never delays messages to the others or
// blocks the Raft thread.
//
// Delivery is best effort, which is all Raft needs: a full queue drops its oldest
// messages, a broken connection drops what was queued, and the leader's next heartbeat
// retransmits whatever the follower is missing.
class PeerTransport {
 public:
  PeerTransport(raft::NodeId self, const std::map<raft::NodeId, std::string>& peers,
                std::shared_ptr<net::TlsContext> tls = nullptr);
  ~PeerTransport();

  PeerTransport(const PeerTransport&) = delete;
  PeerTransport& operator=(const PeerTransport&) = delete;

  // Send queues m for m.to without blocking.
  void Send(const raft::Message& m);

  [[nodiscard]] uint64_t dropped() const;

 private:
  struct Peer {
    std::string addr;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<std::string> queue;  // encoded frames
    uint64_t dropped = 0;
    std::thread thread;
  };

  void Run(Peer* peer);

  static constexpr size_t kMaxQueue = 4096;

  std::shared_ptr<net::TlsContext> tls_;
  std::map<raft::NodeId, std::unique_ptr<Peer>> peers_;
  std::atomic<bool> stopping_{false};
};

}  // namespace kv::server

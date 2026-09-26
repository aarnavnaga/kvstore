#include "server/replica.h"

#include <cstdio>
#include <cstdlib>
#include <sstream>

#include "common/file.h"
#include "net/protocol.h"

namespace kv::server {

using Clock = std::chrono::steady_clock;

std::string ReplicaStatus::ToString() const {
  std::ostringstream os;
  os << "id=" << id << " role=" << raft::RoleName(role) << " term=" << term << " leader=" << leader
     << " commit=" << commit << " applied=" << applied << " last_index=" << last_index
     << " apply_hash=" << apply_hash;
  return os.str();
}

Status Replica::Open(ReplicaOptions options, std::unique_ptr<Replica>* out) {
  if (options.peers.count(options.id) == 0) {
    return Status::InvalidArgument("this node's id is not in the peer list");
  }
  std::unique_ptr<Replica> r(new Replica(std::move(options)));
  const ReplicaOptions& o = r->options_;
  KV_RETURN_IF_ERROR(raft::FileLogStorage::Open(JoinPath(o.data_dir, "raft"), &r->log_));
  KV_RETURN_IF_ERROR(KvStateMachine::Open(o.db, JoinPath(o.data_dir, "db"), &r->sm_));

  raft::Config c;
  c.id = o.id;
  for (const auto& [id, addr] : o.peers) {
    c.peers.push_back(id);
  }
  c.election_ticks_min = o.election_ticks_min;
  c.election_ticks_max = o.election_ticks_max;
  c.heartbeat_ticks = o.heartbeat_ticks;
  c.seed = static_cast<uint64_t>(Clock::now().time_since_epoch().count());
  c.applied = r->sm_->applied_index();
  r->raft_ = std::make_unique<raft::RaftNode>(c, r->log_.get());
  r->PublishStatus();
  *out = std::move(r);
  return Status::OK();
}

Replica::~Replica() { Stop(); }

void Replica::Start() {
  transport_ = std::make_unique<PeerTransport>(options_.id, options_.peers);
  thread_ = std::thread([this] { Run(); });
}

void Replica::Stop() {
  if (stopping_.exchange(true)) {
    return;
  }
  inbox_cv_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
  transport_.reset();
  // Whatever is still waiting will never be answered by this process.
  for (auto& [index, p] : pending_) {
    p.respond(net::Frame{net::MsgType::kResponse, 0,
                         net::EncodeKvResponse(net::ResponseFromStatus(
                             Status::Unavailable("node shutting down; outcome unknown")))});
  }
  pending_.clear();
}

// ---------------------------------------------------------------------------------------
// Network entry

void Replica::Handle(const net::Frame& frame, const net::Responder& respond) {
  auto reply = [&respond](const net::KvResponse& r) {
    respond(net::Frame{net::MsgType::kResponse, 0, net::EncodeKvResponse(r)});
  };
  switch (frame.type) {
    case net::MsgType::kRaft: {
      raft::Message m;
      if (raft::DecodeMessage(frame.payload, &m) && m.to == options_.id) {
        {
          std::lock_guard lk(inbox_mu_);
          inbox_msgs_.push_back(std::move(m));
        }
        inbox_cv_.notify_one();
      }
      return;  // one-way: peers do not wait for a frame-level reply
    }
    case net::MsgType::kPing:
      reply(net::KvResponse{});
      return;
    case net::MsgType::kStatus:
      respond(net::Frame{net::MsgType::kStatus, 0, status().ToString()});
      return;
    case net::MsgType::kPut:
    case net::MsgType::kGet:
    case net::MsgType::kDelete: {
      net::KvRequest req;
      if (!net::DecodeKvRequest(frame.type, frame.payload, &req)) {
        reply(net::ResponseFromStatus(Status::InvalidArgument("malformed request")));
        return;
      }
      if (std::string_view(req.key).substr(0, kReservedPrefix.size()) == kReservedPrefix) {
        reply(net::ResponseFromStatus(Status::InvalidArgument("reserved key prefix")));
        return;
      }
      Proposal p;
      p.cmd = Command{req.op, req.client_id, req.seq, std::move(req.key), std::move(req.value)};
      p.respond = respond;
      p.deadline = Clock::now() + options_.request_timeout;
      {
        std::lock_guard lk(inbox_mu_);
        inbox_proposals_.push_back(std::move(p));
      }
      inbox_cv_.notify_one();
      return;
    }
    default:
      reply(net::ResponseFromStatus(Status::InvalidArgument("unsupported message type")));
  }
}

// ---------------------------------------------------------------------------------------
// Raft thread

void Replica::Run() {
  const auto tick = std::chrono::milliseconds(options_.tick_ms);
  auto next_tick = Clock::now() + tick;
  while (!stopping_) {
    std::deque<raft::Message> msgs;
    std::vector<Proposal> proposals;
    {
      std::unique_lock lk(inbox_mu_);
      inbox_cv_.wait_until(lk, next_tick, [&] {
        return stopping_ || !inbox_msgs_.empty() || !inbox_proposals_.empty();
      });
      msgs.swap(inbox_msgs_);
      proposals.swap(inbox_proposals_);
    }
    if (stopping_) {
      break;
    }

    for (const raft::Message& m : msgs) {
      raft_->Step(m);
    }
    const auto now = Clock::now();
    if (now >= next_tick) {
      raft_->Tick();
      next_tick += tick;
      if (next_tick < now) {
        next_tick = now + tick;  // fell behind (e.g. a slow fsync); don't burst ticks
      }
    }
    if (!proposals.empty()) {
      ProposeBatch(std::move(proposals));
    }
    for (const raft::Message& m : raft_->TakeMessages()) {
      transport_->Send(m);
    }
    ApplyCommitted();
    ExpirePending();
    PublishStatus();
  }
}

void Replica::ProposeBatch(std::vector<Proposal> batch) {
  if (raft_->role() != raft::Role::kLeader) {
    for (const Proposal& p : batch) {
      RespondNotLeader(p.respond);
    }
    return;
  }
  for (size_t start = 0; start < batch.size(); start += options_.max_batch) {
    const size_t end = std::min(batch.size(), start + options_.max_batch);
    std::vector<std::string> data;
    data.reserve(end - start);
    for (size_t i = start; i < end; ++i) {
      data.push_back(EncodeCommand(batch[i].cmd));
    }
    raft::Index first = 0;
    raft::Term term = 0;
    if (!raft_->ProposeBatch(std::move(data), &first, &term).ok()) {
      for (size_t i = start; i < end; ++i) {
        RespondNotLeader(batch[i].respond);
      }
      continue;
    }
    for (size_t i = start; i < end; ++i) {
      pending_[first + (i - start)] = Pending{term, std::move(batch[i].respond), batch[i].deadline};
    }
  }
}

void Replica::ApplyCommitted() {
  const std::vector<raft::Entry> entries = raft_->TakeCommittedEntries();
  if (entries.empty()) {
    return;
  }
  std::vector<ApplyResult> results;
  const Status s = sm_->Apply(entries, &results);
  if (!s.ok()) {
    // The state machine cannot fall behind the log it has acknowledged; fail-stop and
    // let recovery replay from the last durable applied index.
    std::fprintf(stderr, "replica: apply failed: %s\n", s.ToString().c_str());
    std::abort();
  }
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto it = pending_.find(entries[i].index);
    if (it == pending_.end()) {
      continue;
    }
    net::KvResponse resp;
    if (it->second.term != entries[i].term) {
      // Our proposal was overwritten by another leader's entry: it did not commit at
      // this index, and never will. Safe to retry (and deduplicated if it did land).
      resp = net::ResponseFromStatus(Status::Unavailable("leadership changed"));
    } else {
      resp = net::ResponseFromStatus(results[i].status);
      resp.value = std::move(results[i].value);
    }
    it->second.respond(net::Frame{net::MsgType::kResponse, 0, net::EncodeKvResponse(resp)});
    pending_.erase(it);
  }
}

void Replica::ExpirePending() {
  if (pending_.empty()) {
    return;
  }
  const auto now = Clock::now();
  for (auto it = pending_.begin(); it != pending_.end();) {
    if (it->second.deadline <= now) {
      // The entry may still commit later: the outcome is unknown, so say exactly that.
      it->second.respond(net::Frame{net::MsgType::kResponse, 0,
                                    net::EncodeKvResponse(net::ResponseFromStatus(
                                        Status::Timeout("not committed in time")))});
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }
}

void Replica::RespondNotLeader(const net::Responder& respond) const {
  net::KvResponse r;
  r.code = Status::Code::kNotLeader;
  const auto it = options_.peers.find(raft_->leader());
  if (it != options_.peers.end() && it->first != options_.id) {
    r.leader_hint = it->second;
  }
  respond(net::Frame{net::MsgType::kResponse, 0, net::EncodeKvResponse(r)});
}

void Replica::PublishStatus() {
  ReplicaStatus s;
  s.id = options_.id;
  s.role = raft_->role();
  s.term = raft_->term();
  s.leader = raft_->leader();
  s.commit = raft_->commit_index();
  s.applied = sm_->applied_index();
  s.last_index = raft_->last_index();
  s.apply_hash = sm_->apply_hash();
  std::lock_guard lk(status_mu_);
  status_ = s;
}

ReplicaStatus Replica::status() const {
  std::lock_guard lk(status_mu_);
  return status_;
}

}  // namespace kv::server

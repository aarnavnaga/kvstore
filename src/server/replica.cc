#include "server/replica.h"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

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
     << " apply_hash=" << apply_hash << " snapshot_index=" << snapshot_index
     << " snapshots_installed=" << snapshots_installed << " snapshots_sent=" << snapshots_sent
     << " joined=" << (joined ? 1 : 0) << " ticks=" << ticks;
  return os.str();
}

Status Replica::Open(ReplicaOptions options, std::unique_ptr<Replica>* out) {
  if (options.peers.count(options.id) == 0) {
    return Status::InvalidArgument("this node's id is not in the peer list");
  }
  std::unique_ptr<Replica> r(new Replica(std::move(options)));
  const ReplicaOptions& o = r->options_;
  r->wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (r->wake_fd_ < 0) {
    return Status::IOErrorFromErrno("eventfd");
  }
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
  c.join = o.join;
  Replica* self = r.get();
  c.snapshot_source = [self] { return self->SnapshotSource(); };
  c.snapshot_sink = [self](const raft::Snapshot& snap) {
    KV_RETURN_IF_ERROR(self->sm_->RestoreSnapshot(snap));
    self->installed_through_ = snap.meta.index;
    self->cached_snapshot_.reset();
    return Status::OK();
  };
  r->raft_ = std::make_unique<raft::RaftNode>(c, r->log_.get());
  r->PublishStatus();
  *out = std::move(r);
  return Status::OK();
}

Replica::~Replica() {
  Stop();
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
}

void Replica::Wake() {
  const uint64_t one = 1;
  (void)!::write(wake_fd_, &one, sizeof(one));  // a saturated counter still wakes
}

void Replica::Start() {
  transport_ = std::make_unique<PeerTransport>(options_.id, options_.peers, options_.tls);
  thread_ = std::thread([this] { Run(); });
}

void Replica::Stop() {
  if (stopping_.exchange(true)) {
    return;
  }
  Wake();
  if (thread_.joinable()) {
    thread_.join();
  }
  transport_.reset();
  // Whatever is still waiting will never be answered by this process.
  const Status down = Status::Unavailable("node shutting down; outcome unknown");
  for (auto& [index, p] : pending_) {
    Fail(p.respond, down);
  }
  pending_.clear();
  for (auto& [ctx, reads] : unconfirmed_reads_) {
    for (const Read& r : reads) {
      Fail(r.respond, down);
    }
  }
  unconfirmed_reads_.clear();
  for (auto& [index, r] : confirmed_reads_) {
    Fail(r.respond, down);
  }
  confirmed_reads_.clear();
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
        Wake();
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
      const auto deadline = Clock::now() + options_.request_timeout;
      if (req.op == net::MsgType::kGet && !options_.read_through_log) {
        {
          std::lock_guard lk(inbox_mu_);
          inbox_reads_.push_back(Read{std::move(req.key), respond, deadline});
        }
        Wake();
        return;
      }
      Proposal p;
      p.cmd = Command{req.op, req.client_id, req.seq, std::move(req.key), std::move(req.value)};
      p.respond = respond;
      p.deadline = deadline;
      {
        std::lock_guard lk(inbox_mu_);
        inbox_proposals_.push_back(std::move(p));
      }
      Wake();
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
    std::vector<Read> reads;
    const auto wait =
        std::chrono::duration_cast<std::chrono::milliseconds>(next_tick - Clock::now());
    pollfd p{wake_fd_, POLLIN, 0};
    if (::poll(&p, 1, static_cast<int>(std::max<int64_t>(0, wait.count()))) > 0) {
      uint64_t drained = 0;
      (void)!::read(wake_fd_, &drained, sizeof(drained));
    }
    {
      std::lock_guard lk(inbox_mu_);
      msgs.swap(inbox_msgs_);
      proposals.swap(inbox_proposals_);
      reads.swap(inbox_reads_);
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
      ++ticks_;
      next_tick += tick;
      if (next_tick < now) {
        next_tick = now + tick;  // fell behind (e.g. a slow fsync); don't burst ticks
      }
    }
    if (!proposals.empty()) {
      ProposeBatch(std::move(proposals));
    }
    if (!reads.empty()) {
      StartReads(std::move(reads));
    }
    for (const raft::Message& m : raft_->TakeMessages()) {
      transport_->Send(m);
    }
    ApplyCommitted();
    ServeReads();
    MaybeCompact();
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
    proposals_ += end - start;
    ++proposal_batches_;
  }
}

void Replica::ApplyCommitted() {
  if (installed_through_ > 0) {
    // A snapshot replaced entries we were waiting on; their outcome is unknown here.
    while (!pending_.empty() && pending_.begin()->first <= installed_through_) {
      Fail(pending_.begin()->second.respond,
           Status::Unavailable("superseded by a snapshot; outcome unknown"));
      pending_.erase(pending_.begin());
    }
    installed_through_ = 0;
  }
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

// ---------------------------------------------------------------------------------------
// Linearizable reads (ReadIndex)

void Replica::StartReads(std::vector<Read> reads) {
  if (options_.unsafe_local_reads) {
    for (Read& r : reads) {
      confirmed_reads_.emplace(0, std::move(r));  // served right away, from whatever we have
    }
    return;
  }
  const uint64_t ctx = ++next_read_ctx_;
  // One ReadIndex round (one heartbeat to every follower) covers every GET that arrived
  // since the last loop iteration.
  if (!raft_->ReadIndex(ctx).ok()) {
    for (const Read& r : reads) {
      RespondNotLeader(r.respond);
    }
    return;
  }
  unconfirmed_reads_[ctx] = std::move(reads);
}

void Replica::ServeReads() {
  for (const auto& rs : raft_->TakeReadStates()) {
    const auto it = unconfirmed_reads_.find(rs.ctx);
    if (it == unconfirmed_reads_.end()) {
      continue;
    }
    for (Read& r : it->second) {
      confirmed_reads_.emplace(rs.index, std::move(r));
    }
    unconfirmed_reads_.erase(it);
  }
  if (raft_->role() != raft::Role::kLeader && !unconfirmed_reads_.empty()) {
    // Leadership lost before the heartbeat round confirmed these; retrying is safe.
    for (auto& [ctx, reads] : unconfirmed_reads_) {
      for (const Read& r : reads) {
        RespondNotLeader(r.respond);
      }
    }
    unconfirmed_reads_.clear();
  }
  // A confirmed read is served once the state machine has caught up to its read index.
  // That holds even if leadership was lost since: the read was linearizable at the
  // moment of confirmation, which lies between its invocation and this response.
  const raft::Index applied = sm_->applied_index();
  while (!confirmed_reads_.empty() && confirmed_reads_.begin()->first <= applied) {
    Read r = std::move(confirmed_reads_.begin()->second);
    confirmed_reads_.erase(confirmed_reads_.begin());
    net::KvResponse resp;
    const Status s = sm_->Get(r.key, &resp.value);
    if (!s.ok()) {
      resp = net::ResponseFromStatus(s);
    }
    r.respond(net::Frame{net::MsgType::kResponse, 0, net::EncodeKvResponse(resp)});
  }
}

// ---------------------------------------------------------------------------------------
// Snapshots

std::shared_ptr<const raft::Snapshot> Replica::SnapshotSource() {
  if (cached_snapshot_ && cached_snapshot_->meta.index == sm_->applied_index()) {
    return cached_snapshot_;
  }
  auto snap = std::make_shared<raft::Snapshot>();
  const Status s = sm_->CreateSnapshot(snap.get());
  if (!s.ok()) {
    std::fprintf(stderr, "replica: snapshot failed: %s\n", s.ToString().c_str());
    return nullptr;
  }
  cached_snapshot_ = std::move(snap);
  return cached_snapshot_;
}

void Replica::MaybeCompact() {
  const raft::Index applied = sm_->applied_index();
  const raft::Index snap = log_->Snapshot().index;
  if (applied < snap + options_.snapshot_threshold || applied <= options_.snapshot_keep) {
    return;
  }
  // The DB is unsynced; make the applied state durable before the log forgets it.
  Status s = sm_->Sync();
  if (s.ok()) {
    s = raft_->Compact(applied - options_.snapshot_keep);
  }
  if (!s.ok()) {
    std::fprintf(stderr, "replica: compaction failed: %s\n", s.ToString().c_str());
    std::abort();
  }
}

void Replica::ExpirePending() {
  const auto now = Clock::now();
  for (auto it = confirmed_reads_.begin(); it != confirmed_reads_.end();) {
    if (it->second.deadline <= now) {
      Fail(it->second.respond, Status::Timeout("read not served in time"));
      it = confirmed_reads_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto& [ctx, reads] : unconfirmed_reads_) {
    for (auto it = reads.begin(); it != reads.end();) {
      if (it->deadline <= now) {
        Fail(it->respond, Status::Timeout("read not confirmed in time"));
        it = reads.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto it = pending_.begin(); it != pending_.end();) {
    if (it->second.deadline <= now) {
      // The entry may still commit later: the outcome is unknown, so say exactly that.
      Fail(it->second.respond, Status::Timeout("not committed in time"));
      it = pending_.erase(it);
    } else {
      ++it;
    }
  }
}

void Replica::Fail(const net::Responder& respond, const Status& s) const {
  respond(
      net::Frame{net::MsgType::kResponse, 0, net::EncodeKvResponse(net::ResponseFromStatus(s))});
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
  s.snapshot_index = log_->Snapshot().index;
  s.snapshots_installed = raft_->snapshots_installed();
  s.snapshots_sent = raft_->snapshots_sent();
  s.joined = raft_->joined();
  s.ticks = ticks_;
  if (raft_->leader() != last_leader_) {
    if (raft_->leader() != raft::kNoNode) {
      ++leader_changes_;
    }
    last_leader_ = raft_->leader();
  }
  s.leader_changes = leader_changes_;
  s.proposals = proposals_;
  s.proposal_batches = proposal_batches_;
  s.log_appends = log_->appends();
  s.log_sync_seconds = static_cast<double>(log_->sync_nanos()) / 1e9;
  s.peer_messages_dropped = transport_ ? transport_->dropped() : 0;
  std::lock_guard lk(status_mu_);
  status_ = s;
}

ReplicaStatus Replica::status() const {
  std::lock_guard lk(status_mu_);
  return status_;
}

}  // namespace kv::server

#include "raft/raft.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

namespace kv::raft {

const char* RoleName(Role role) {
  switch (role) {
    case Role::kFollower:
      return "follower";
    case Role::kPreCandidate:
      return "pre-candidate";
    case Role::kCandidate:
      return "candidate";
    case Role::kLeader:
      return "leader";
  }
  return "unknown";
}

RaftNode::RaftNode(Config config, LogStorage* storage)
    : config_(std::move(config)), storage_(storage), rng_(config_.seed ^ config_.id) {
  const HardState hs = storage_->LoadHardState();
  term_ = hs.term;
  vote_ = hs.vote;
  // Nothing at or below the snapshot needs applying, and the state machine tells us what
  // it already applied. Commit is not persisted: a node relearns it from the leader.
  applied_ = std::max(config_.applied, storage_->Snapshot().index);
  commit_ = applied_;
  joined_ = !(config_.join || hs.joining);
  if (!joined_ && !hs.joining) {
    SetTerm(term_, vote_);  // persists joining=true before anything else happens
  }
  ResetElectionTimer();
}

void RaftNode::Fatal(const Status& s) {
  std::fprintf(stderr, "raft: fatal storage error: %s\n", s.ToString().c_str());
  std::abort();
}

// ---------------------------------------------------------------------------------------
// Role transitions

void RaftNode::BecomeFollower(Term term, NodeId leader) {
  if (term > term_) {
    SetTerm(term, kNoNode);
  }
  role_ = Role::kFollower;
  leader_ = leader;
  votes_.clear();
  progress_.clear();
  // Reads in flight lose their leadership confirmation; the owner sees the role change
  // and fails them (they are safe to retry).
  queued_reads_.clear();
  pending_reads_.clear();
  ResetElectionTimer();
}

void RaftNode::BecomePreCandidate() {
  // A pre-candidate changes neither its term nor its vote, so losing a PreVote round
  // leaves no trace on the cluster.
  role_ = Role::kPreCandidate;
  leader_ = kNoNode;
  votes_.clear();
  ResetElectionTimer();
}

void RaftNode::BecomeCandidate() {
  role_ = Role::kCandidate;
  leader_ = kNoNode;
  votes_.clear();
  SetTerm(term_ + 1, config_.id);  // vote for ourselves, durably, before asking others
  ResetElectionTimer();
}

void RaftNode::BecomeLeader() {
  role_ = Role::kLeader;
  leader_ = config_.id;
  votes_.clear();
  progress_.clear();
  heartbeat_elapsed_ = 0;
  election_elapsed_ = 0;
  const Index last = storage_->LastIndex();
  for (const NodeId peer : config_.peers) {
    if (peer != config_.id) {
      progress_[peer] = Progress{last + 1, 0, true};
    }
  }
  // A leader may only commit entries from its own term by counting replicas (Figure 8 of
  // the paper). Appending a no-op lets it commit everything before it promptly, which is
  // also what makes ReadIndex safe.
  AppendToLog({Entry{term_, last + 1, EntryType::kNoop, {}}});
  term_start_index_ = last + 1;
  MaybeAdvanceCommit();  // a single-node cluster commits on its own
  BroadcastAppend();
}

// ---------------------------------------------------------------------------------------
// Clock

void RaftNode::Tick() {
  if (role_ == Role::kLeader) {
    for (auto& [peer, pr] : progress_) {
      if (pr.snapshot) {
        ++pr.snapshot_idle_ticks;
      }
    }
    ++heartbeat_elapsed_;
    ++election_elapsed_;
    if (config_.check_quorum && election_elapsed_ >= config_.election_ticks_min) {
      election_elapsed_ = 0;
      if (!CheckQuorumActive()) {
        // Cut off from a majority: we can no longer commit anything, and a new leader
        // may already exist on the other side. Stop answering as leader.
        BecomeFollower(term_, kNoNode);
        return;
      }
    }
    if (heartbeat_elapsed_ >= config_.heartbeat_ticks) {
      heartbeat_elapsed_ = 0;
      BroadcastAppend();
    }
    return;
  }
  if (++election_elapsed_ >= randomized_election_ticks_) {
    if (!joined_) {
      ResetElectionTimer();  // a joining node never campaigns; it waits for a leader
      return;
    }
    StartElection(config_.pre_vote);
  }
}

void RaftNode::ResetElectionTimer() {
  election_elapsed_ = 0;
  std::uniform_int_distribution<int> dist(config_.election_ticks_min, config_.election_ticks_max);
  randomized_election_ticks_ = dist(rng_);
}

void RaftNode::Campaign() { StartElection(config_.pre_vote); }

void RaftNode::StartElection(bool pre_vote) {
  if (pre_vote) {
    BecomePreCandidate();
  } else {
    BecomeCandidate();
  }
  votes_[config_.id] = true;
  if (votes_.size() >= Quorum()) {  // single-node cluster
    if (pre_vote) {
      StartElection(false);
    } else {
      BecomeLeader();
    }
    return;
  }
  const Index last = storage_->LastIndex();
  const Term last_term = storage_->TermAt(last);
  for (const NodeId peer : config_.peers) {
    if (peer == config_.id) {
      continue;
    }
    Message m;
    m.kind = MsgKind::kVoteRequest;
    m.to = peer;
    // A PreVote carries the term the node *would* campaign in, without adopting it.
    m.term = pre_vote ? term_ + 1 : term_;
    m.pre_vote = pre_vote;
    m.last_log_index = last;
    m.last_log_term = last_term;
    Send(std::move(m));
  }
}

// ---------------------------------------------------------------------------------------
// Message dispatch

void RaftNode::Step(const Message& m) {
  if (m.term > term_) {
    const bool vote_req = m.kind == MsgKind::kVoteRequest;
    if (vote_req && config_.check_quorum && leader_ != kNoNode &&
        election_elapsed_ < config_.election_ticks_min) {
      // We heard from a live leader within the minimum election timeout, so this
      // candidate is either partitioned or impatient. Ignoring it (without adopting its
      // term) keeps one flaky node from deposing a healthy leader.
      return;
    }
    // A PreVote request never changes anyone's term, and a granted PreVote response
    // merely echoes the prospective term back; neither is evidence of a newer term.
    const bool prospective_term =
        (vote_req && m.pre_vote) || (m.kind == MsgKind::kVoteResponse && m.pre_vote && m.granted);
    if (!prospective_term) {
      const bool from_leader =
          m.kind == MsgKind::kAppendRequest || m.kind == MsgKind::kSnapshotRequest;
      BecomeFollower(m.term, from_leader ? m.from : kNoNode);
    }
  } else if (m.term < term_) {
    // A stale leader or pre-candidate learns our newer term from the reply and steps
    // down. Everything else from an old term is ignored.
    if (m.kind == MsgKind::kAppendRequest || m.kind == MsgKind::kSnapshotRequest) {
      Message r;
      r.kind = MsgKind::kAppendResponse;
      r.to = m.from;
      r.success = false;
      Send(std::move(r));
    } else if (m.kind == MsgKind::kVoteRequest && m.pre_vote) {
      Message r;
      r.kind = MsgKind::kVoteResponse;
      r.to = m.from;
      r.pre_vote = true;
      r.granted = false;
      Send(std::move(r));
    }
    return;
  }

  switch (m.kind) {
    case MsgKind::kVoteRequest:
      HandleVoteRequest(m);
      break;
    case MsgKind::kVoteResponse:
      HandleVoteResponse(m);
      break;
    case MsgKind::kAppendRequest:
      HandleAppendRequest(m);
      break;
    case MsgKind::kAppendResponse:
      HandleAppendResponse(m);
      break;
    case MsgKind::kSnapshotRequest:
      HandleSnapshotRequest(m);
      break;
    case MsgKind::kSnapshotResponse:
      HandleSnapshotResponse(m);
      break;
  }
}

// ---------------------------------------------------------------------------------------
// Elections

bool RaftNode::LogUpToDate(Index last_index, Term last_term) const {
  // Election restriction (section 5.4.1): only vote for a candidate whose log is at least
  // as up to date as ours, so a new leader always holds every committed entry.
  const Index my_last = storage_->LastIndex();
  const Term my_term = storage_->TermAt(my_last);
  return last_term > my_term || (last_term == my_term && last_index >= my_last);
}

void RaftNode::HandleVoteRequest(const Message& m) {
  // m.term == term_ here, except for a PreVote, which may carry term_ + 1.
  const bool can_vote = vote_ == m.from ||                           // repeat of our vote
                        (vote_ == kNoNode && leader_ == kNoNode) ||  // free to vote
                        (m.pre_vote && m.term > term_);              // PreVote for a future term
  const bool grant = joined_ && can_vote && LogUpToDate(m.last_log_index, m.last_log_term);

  if (grant && !m.pre_vote) {
    // The vote is durable before the response is queued: a node that votes, crashes and
    // restarts must not vote for someone else in the same term.
    SetTerm(term_, m.from);
    ResetElectionTimer();
  }
  Message r;
  r.kind = MsgKind::kVoteResponse;
  r.to = m.from;
  r.pre_vote = m.pre_vote;
  r.granted = grant;
  // A granted PreVote echoes the candidate's prospective term so it can match the
  // response to its round; everything else carries our real term.
  r.term = (m.pre_vote && grant) ? m.term : term_;
  Send(std::move(r));
}

void RaftNode::HandleVoteResponse(const Message& m) {
  const bool expected =
      (role_ == Role::kPreCandidate && m.pre_vote) || (role_ == Role::kCandidate && !m.pre_vote);
  if (!expected) {
    return;  // a straggler from an earlier round
  }
  votes_[m.from] = m.granted;
  size_t granted = 0;
  for (const auto& [id, g] : votes_) {
    granted += g ? 1 : 0;
  }
  const size_t rejected = votes_.size() - granted;
  if (granted >= Quorum()) {
    if (role_ == Role::kPreCandidate) {
      StartElection(false);
    } else {
      BecomeLeader();
    }
  } else if (rejected >= Quorum()) {
    BecomeFollower(term_, kNoNode);
  }
}

// ---------------------------------------------------------------------------------------
// Replication

void RaftNode::HandleAppendRequest(const Message& m) {
  // A valid leader for our term exists, so any campaign of ours is over.
  if (role_ != Role::kFollower) {
    BecomeFollower(term_, m.from);
  }
  leader_ = m.from;
  election_elapsed_ = 0;

  Message r;
  r.kind = MsgKind::kAppendResponse;
  r.to = m.from;
  r.read_ctx = m.read_ctx;

  if (m.prev_log_index < commit_) {
    // Everything up to our commit index is settled; tell the leader where we are.
    r.success = true;
    r.match_index = commit_;
    Send(std::move(r));
    return;
  }

  const Index last = storage_->LastIndex();
  if (m.prev_log_index > last || storage_->TermAt(m.prev_log_index) != m.prev_log_term) {
    // Log Matching check failed. Suggest where the leader should retry: just past our
    // log, or the first index of the conflicting term so a whole stale term is skipped
    // in one round trip rather than one entry per round trip.
    r.success = false;
    if (m.prev_log_index > last) {
      r.reject_hint = last + 1;
    } else {
      const Term conflict = storage_->TermAt(m.prev_log_index);
      Index i = m.prev_log_index;
      while (i > commit_ + 1 && i > storage_->FirstIndex() && storage_->TermAt(i - 1) == conflict) {
        --i;
      }
      r.reject_hint = i;
    }
    Send(std::move(r));
    return;
  }

  // Skip entries we already have; truncate only at the first real conflict. A delayed,
  // duplicated append must never truncate entries that a newer append already added.
  size_t start = 0;
  for (; start < m.entries.size(); ++start) {
    const Entry& e = m.entries[start];
    if (e.index > storage_->LastIndex() || storage_->TermAt(e.index) != e.term) {
      break;
    }
  }
  if (start < m.entries.size()) {
    if (m.entries[start].index <= commit_) {
      Fatal(Status::Corruption("leader tried to overwrite a committed entry"));
    }
    AppendToLog(std::vector<Entry>(m.entries.begin() + static_cast<std::ptrdiff_t>(start),
                                   m.entries.end()));
  }

  // Only entries this append vouched for may be committed: our log could hold a stale
  // suffix beyond them that the leader has not checked yet.
  const Index last_new = m.prev_log_index + m.entries.size();
  if (m.commit > commit_) {
    commit_ = std::min(m.commit, last_new);
  }
  MaybeJoin();
  r.success = true;
  r.match_index = last_new;
  Send(std::move(r));
}

void RaftNode::MaybeJoin() {
  // Committing an entry of the current leader's term means our log now contains every
  // entry committed before it (Log Matching), including any we forgot when wiped.
  if (!joined_ && leader_ != kNoNode && commit_ > 0 && commit_ <= storage_->LastIndex() &&
      storage_->TermAt(commit_) == term_) {
    joined_ = true;
    SetTerm(term_, vote_);  // clear the durable joining flag
  }
}

void RaftNode::HandleAppendResponse(const Message& m) {
  if (role_ != Role::kLeader) {
    return;
  }
  const auto it = progress_.find(m.from);
  if (it == progress_.end()) {
    return;
  }
  Progress& pr = it->second;
  pr.recent_active = true;

  // Any response in our term, success or not, confirms we were still leader when the
  // heartbeat carrying read_ctx went out, which covers every read up to that ctx.
  if (m.read_ctx != 0 && !pending_reads_.empty()) {
    for (PendingRead& r : pending_reads_) {
      if (r.ctx <= m.read_ctx) {
        r.acks.insert(m.from);
      }
    }
    while (!pending_reads_.empty() && pending_reads_.front().acks.size() >= Quorum()) {
      ready_reads_.push_back({pending_reads_.front().ctx, pending_reads_.front().index});
      pending_reads_.pop_front();
    }
  }

  if (m.success) {
    if (m.match_index > pr.match) {
      pr.match = m.match_index;
      MaybeAdvanceCommit();
    }
    pr.next = std::max(pr.next, pr.match + 1);
    if (pr.next <= storage_->LastIndex()) {
      SendAppend(m.from);  // more to send
    }
    return;
  }
  // Rejected: back up to the follower's hint. Normally the hint is above what the
  // follower already matched, but a follower whose disk was wiped has lost entries it
  // once acknowledged. Lowering match is always safe (it only makes commit counting
  // more conservative, and commit never moves backwards); clamping next to the old match
  // instead would resend the same rejected append forever.
  pr.next = std::max<Index>(1, std::min(m.reject_hint, pr.next));
  if (pr.next <= pr.match) {
    pr.match = pr.next - 1;
  }
  SendAppend(m.from);
}

void RaftNode::BroadcastAppend() {
  for (const auto& [peer, pr] : progress_) {
    SendAppend(peer);
  }
}

void RaftNode::SendAppend(NodeId to) {
  Progress& pr = progress_[to];
  const Index prev = pr.next - 1;
  if (pr.snapshot) {
    // A transfer is in flight: retransmit its chunk only if it has gone unanswered.
    if (pr.snapshot_idle_ticks >= config_.election_ticks_min / 2) {
      SendSnapshot(to);
    }
    return;
  }
  if (prev < storage_->Snapshot().index) {
    // The entries this follower needs were compacted away: send the snapshot instead.
    SendSnapshot(to);
    return;
  }
  Message m;
  m.kind = MsgKind::kAppendRequest;
  m.to = to;
  m.prev_log_index = prev;
  m.prev_log_term = storage_->TermAt(prev);
  m.commit = commit_;
  m.read_ctx = pending_reads_.empty() ? 0 : latest_read_ctx_;
  storage_->GetEntries(pr.next, storage_->LastIndex() + 1, config_.max_append_bytes, &m.entries);
  if (!m.entries.empty()) {
    // Pipeline: assume delivery and keep streaming. A loss shows up as a rejection,
    // which resets next to the follower's hint.
    pr.next = m.entries.back().index + 1;
  }
  Send(std::move(m));
}

void RaftNode::MaybeAdvanceCommit() {
  if (role_ != Role::kLeader) {
    return;
  }
  std::vector<Index> matched{storage_->LastIndex()};
  for (const auto& [peer, pr] : progress_) {
    matched.push_back(pr.match);
  }
  std::sort(matched.begin(), matched.end(), std::greater<>());
  const Index candidate = matched[Quorum() - 1];  // replicated on a majority
  // Only count replicas for an entry of the current term (Figure 8). Earlier entries
  // commit indirectly once one from this term does.
  if (candidate > commit_ && storage_->TermAt(candidate) == term_) {
    commit_ = candidate;
  }
  if (!queued_reads_.empty() && commit_ >= term_start_index_) {
    StartPendingReads();
  }
}

// ---------------------------------------------------------------------------------------
// ReadIndex

Status RaftNode::ReadIndex(uint64_t ctx) {
  if (role_ != Role::kLeader) {
    return Status::NotLeader();
  }
  queued_reads_.push_back(ctx);
  // Until this term's no-op commits, our commit index may lag entries a previous leader
  // committed; a read index taken now could miss them.
  if (commit_ >= term_start_index_) {
    StartPendingReads();
  }
  return Status::OK();
}

void RaftNode::StartPendingReads() {
  for (const uint64_t ctx : queued_reads_) {
    if (Quorum() == 1) {
      ready_reads_.push_back({ctx, commit_});
      continue;
    }
    pending_reads_.push_back(PendingRead{ctx, commit_, {config_.id}});
    latest_read_ctx_ = std::max(latest_read_ctx_, ctx);
  }
  queued_reads_.clear();
  if (!pending_reads_.empty()) {
    BroadcastAppend();  // the heartbeat round that confirms leadership
  }
}

std::vector<RaftNode::ReadState> RaftNode::TakeReadStates() {
  std::vector<ReadState> out;
  out.swap(ready_reads_);
  return out;
}

// ---------------------------------------------------------------------------------------
// Snapshots

Status RaftNode::Compact(Index index) {
  if (index > applied_) {
    return Status::InvalidArgument("cannot compact past the applied index");
  }
  if (index <= storage_->Snapshot().index) {
    return Status::OK();
  }
  return storage_->Compact(SnapshotMeta{index, storage_->TermAt(index)});
}

void RaftNode::SendSnapshot(NodeId to) {
  Progress& pr = progress_[to];
  if (!pr.snapshot) {
    if (!config_.snapshot_source) {
      return;
    }
    pr.snapshot = config_.snapshot_source();
    pr.snapshot_offset = 0;
    if (!pr.snapshot) {
      return;
    }
  }
  pr.snapshot_idle_ticks = 0;
  const std::string& data = pr.snapshot->data;
  const uint64_t offset = std::min<uint64_t>(pr.snapshot_offset, data.size());
  const size_t len = std::min<size_t>(config_.snapshot_chunk_bytes, data.size() - offset);
  Message m;
  m.kind = MsgKind::kSnapshotRequest;
  m.to = to;
  m.snapshot = pr.snapshot->meta;
  m.offset = offset;
  m.data = data.substr(offset, len);
  m.done = offset + len == data.size();
  m.commit = commit_;
  Send(std::move(m));
}

void RaftNode::HandleSnapshotRequest(const Message& m) {
  if (role_ != Role::kFollower) {
    BecomeFollower(term_, m.from);
  }
  leader_ = m.from;
  election_elapsed_ = 0;

  Message r;
  r.kind = MsgKind::kSnapshotResponse;
  r.to = m.from;
  r.snapshot = m.snapshot;

  if (m.snapshot.index <= commit_) {
    // We already have everything it covers; just tell the leader where we are.
    incoming_snapshot_.reset();
    r.success = true;
    r.done = true;
    r.match_index = commit_;
    Send(std::move(r));
    return;
  }
  if (m.offset == 0) {
    incoming_snapshot_ = Snapshot{m.snapshot, {}};
  } else if (!incoming_snapshot_ || incoming_snapshot_->meta.index != m.snapshot.index ||
             incoming_snapshot_->meta.term != m.snapshot.term ||
             incoming_snapshot_->data.size() != m.offset) {
    // A chunk out of sequence (lost or reordered): ask for the byte we expect next.
    const bool same = incoming_snapshot_ && incoming_snapshot_->meta.index == m.snapshot.index &&
                      incoming_snapshot_->meta.term == m.snapshot.term;
    r.success = false;
    r.offset = same ? incoming_snapshot_->data.size() : 0;
    Send(std::move(r));
    return;
  }
  incoming_snapshot_->data.append(m.data);
  if (!m.done) {
    r.success = true;
    r.offset = incoming_snapshot_->data.size();
    Send(std::move(r));
    return;
  }

  // Complete. The state machine takes it first (durably); only then does the log forget
  // what the snapshot replaces.
  const Snapshot snap = std::move(*incoming_snapshot_);
  incoming_snapshot_.reset();
  if (!config_.snapshot_sink) {
    Fatal(Status::InvalidArgument("received a snapshot but no snapshot_sink is configured"));
  }
  Status s = config_.snapshot_sink(snap);
  if (!s.ok()) {
    Fatal(s);
  }
  if (storage_->LastIndex() >= snap.meta.index &&
      storage_->TermAt(snap.meta.index) == snap.meta.term) {
    s = storage_->Compact(snap.meta);  // our log continues the snapshot: keep the suffix
  } else {
    s = storage_->ResetToSnapshot(snap.meta);
  }
  if (!s.ok()) {
    Fatal(s);
  }
  commit_ = std::max(commit_, snap.meta.index);
  applied_ = snap.meta.index;
  ++snapshots_installed_;
  MaybeJoin();
  r.success = true;
  r.done = true;
  r.match_index = snap.meta.index;
  Send(std::move(r));
}

void RaftNode::HandleSnapshotResponse(const Message& m) {
  if (role_ != Role::kLeader) {
    return;
  }
  const auto it = progress_.find(m.from);
  if (it == progress_.end()) {
    return;
  }
  Progress& pr = it->second;
  pr.recent_active = true;
  if (m.success && m.done) {
    if (pr.snapshot && pr.snapshot->meta.index == m.snapshot.index) {
      pr.snapshot.reset();
      ++snapshots_sent_;
    }
    if (m.match_index > pr.match) {
      pr.match = m.match_index;
      MaybeAdvanceCommit();
    }
    pr.next = std::max(pr.next, pr.match + 1);
    SendAppend(m.from);
    return;
  }
  if (!pr.snapshot || pr.snapshot->meta.index != m.snapshot.index ||
      pr.snapshot->meta.term != m.snapshot.term) {
    return;  // a response to a transfer we have since abandoned
  }
  if (m.success && m.offset <= pr.snapshot_offset) {
    return;  // duplicate or stale ack for a chunk already acknowledged
  }
  if (!m.success && m.offset == pr.snapshot_offset) {
    return;  // the follower wants the chunk we are already waiting on
  }
  pr.snapshot_offset = m.offset;  // next chunk on success, the expected one on a reject
  SendSnapshot(m.from);
}

bool RaftNode::CheckQuorumActive() {
  size_t active = 1;  // ourselves
  for (auto& [peer, pr] : progress_) {
    if (pr.recent_active) {
      ++active;
    }
    pr.recent_active = false;
  }
  return active >= Quorum();
}

Status RaftNode::Propose(std::string data, Index* index, Term* term) {
  std::vector<std::string> one;
  one.push_back(std::move(data));
  return ProposeBatch(std::move(one), index, term);
}

Status RaftNode::ProposeBatch(std::vector<std::string> data, Index* first, Term* term) {
  if (role_ != Role::kLeader) {
    return Status::NotLeader();
  }
  if (data.empty()) {
    return Status::InvalidArgument("empty proposal batch");
  }
  const Index next = storage_->LastIndex() + 1;
  std::vector<Entry> entries;
  entries.reserve(data.size());
  for (auto& d : data) {
    entries.push_back(Entry{term_, next + entries.size(), EntryType::kNormal, std::move(d)});
  }
  AppendToLog(entries);
  *first = next;
  *term = term_;
  MaybeAdvanceCommit();
  BroadcastAppend();
  return Status::OK();
}

std::vector<Entry> RaftNode::TakeCommittedEntries() {
  std::vector<Entry> out;
  if (commit_ > applied_) {
    storage_->GetEntries(applied_ + 1, commit_ + 1, SIZE_MAX, &out);
    if (!out.empty()) {
      applied_ = out.back().index;
    }
  }
  return out;
}

// ---------------------------------------------------------------------------------------
// Helpers

std::string RaftNode::DebugString() const {
  std::string out;
  for (const auto& [peer, pr] : progress_) {
    out += " [" + std::to_string(peer) + ": next=" + std::to_string(pr.next) +
           " match=" + std::to_string(pr.match);
    if (pr.snapshot) {
      out += " snap=" + std::to_string(pr.snapshot->meta.index) + "@" +
             std::to_string(pr.snapshot_offset) + "/" + std::to_string(pr.snapshot->data.size()) +
             " idle=" + std::to_string(pr.snapshot_idle_ticks);
    }
    out += "]";
  }
  return out;
}

void RaftNode::Send(Message m) {
  m.from = config_.id;
  if (m.term == 0) {
    m.term = term_;
  }
  outbox_.push_back(std::move(m));
}

std::vector<Message> RaftNode::TakeMessages() {
  std::vector<Message> out;
  out.swap(outbox_);
  return out;
}

void RaftNode::SetTerm(Term term, NodeId vote) {
  const Status s = storage_->SaveHardState(HardState{term, vote, !joined_});
  if (!s.ok()) {
    Fatal(s);
  }
  term_ = term;
  vote_ = vote;
}

void RaftNode::AppendToLog(const std::vector<Entry>& entries) {
  const Status s = storage_->Append(entries);
  if (!s.ok()) {
    Fatal(s);
  }
}

}  // namespace kv::raft

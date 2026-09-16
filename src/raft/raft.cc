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
  MaybeAdvanceCommit();  // a single-node cluster commits on its own
  BroadcastAppend();
}

// ---------------------------------------------------------------------------------------
// Clock

void RaftNode::Tick() {
  if (role_ == Role::kLeader) {
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
    case MsgKind::kSnapshotResponse:
      break;  // M6
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
  const bool grant = can_vote && LogUpToDate(m.last_log_index, m.last_log_term);

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
  r.success = true;
  r.match_index = last_new;
  Send(std::move(r));
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
  // Rejected: back up to the follower's hint, but never below what it already matched.
  pr.next = std::max(pr.match + 1, std::min(m.reject_hint, pr.next));
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
  Message m;
  m.kind = MsgKind::kAppendRequest;
  m.to = to;
  m.prev_log_index = prev;
  m.prev_log_term = storage_->TermAt(prev);
  m.commit = commit_;
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
  const Status s = storage_->SaveHardState(HardState{term, vote});
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

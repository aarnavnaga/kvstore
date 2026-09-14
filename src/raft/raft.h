#pragma once

#include <cstdint>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "raft/storage.h"
#include "raft/types.h"

namespace kv::raft {

struct Config {
  NodeId id = kNoNode;
  // Every voting member, including this node. Membership is static.
  std::vector<NodeId> peers;

  // Election timeouts are drawn uniformly from [election_ticks_min, election_ticks_max]
  // after every reset, so two nodes rarely time out together and split the vote.
  int election_ticks_min = 10;
  int election_ticks_max = 20;
  int heartbeat_ticks = 2;

  // PreVote: a node that has been cut off first asks whether it *could* win before
  // bumping its term, so rejoining after a partition does not depose a healthy leader.
  bool pre_vote = true;
  // CheckQuorum: a leader that has not heard from a majority for an election timeout
  // steps down, so a leader stranded on the minority side of a partition stops acting
  // as one. It also lets followers ignore vote requests while they have a live leader.
  bool check_quorum = true;

  size_t max_append_bytes = 1 << 20;
  uint64_t seed = 1;
  // The state machine's last applied index, from its own durable state. Entries up to
  // here are never handed out again for applying.
  Index applied = 0;
};

enum class Role { kFollower, kPreCandidate, kCandidate, kLeader };
const char* RoleName(Role role);

// RaftNode is the Raft consensus algorithm as a deterministic state machine. It has no
// threads, clock or sockets: the owner calls Tick() at a fixed interval, feeds incoming
// messages to Step(), and drains outgoing messages and committed entries. That is what
// lets the exact same code run inside a seeded single-process simulator and inside the
// real server.
//
// Durability: every change to the term, vote or log goes to LogStorage synchronously,
// before any message that depends on it is queued, so a node never tells a peer
// something it could forget in a crash. A storage failure aborts the process: continuing
// after a failed fsync could violate safety (fail-stop, as etcd and others do).
class RaftNode {
 public:
  RaftNode(Config config, LogStorage* storage);

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // Tick advances the logical clock by one interval.
  void Tick();
  // Step processes one message from a peer.
  void Step(const Message& m);
  // Campaign starts an election now instead of waiting for the timeout (tests, and
  // leadership transfer).
  void Campaign();

  // Propose appends a command to the leader's log. Returns NotLeader on a follower.
  Status Propose(std::string data, Index* index, Term* term);

  // TakeMessages returns and clears the messages queued for peers.
  std::vector<Message> TakeMessages();
  // TakeCommittedEntries returns committed entries not yet handed out, in log order.
  std::vector<Entry> TakeCommittedEntries();

  [[nodiscard]] NodeId id() const { return config_.id; }
  [[nodiscard]] Role role() const { return role_; }
  [[nodiscard]] Term term() const { return term_; }
  [[nodiscard]] NodeId leader() const { return leader_; }
  [[nodiscard]] Index commit_index() const { return commit_; }
  [[nodiscard]] Index last_index() const { return storage_->LastIndex(); }
  [[nodiscard]] Index applied_index() const { return applied_; }

 private:
  struct Progress {
    Index next = 1;   // next entry to send
    Index match = 0;  // highest entry known to be replicated
    bool recent_active = false;
  };

  void BecomeFollower(Term term, NodeId leader);
  void BecomePreCandidate();
  void BecomeCandidate();
  void BecomeLeader();

  void StartElection(bool pre_vote);
  void HandleVoteRequest(const Message& m);
  void HandleVoteResponse(const Message& m);
  void HandleAppendRequest(const Message& m);
  void HandleAppendResponse(const Message& m);

  void BroadcastAppend();
  void SendAppend(NodeId to);
  void MaybeAdvanceCommit();
  bool CheckQuorumActive();

  void Send(Message m);
  void SetTerm(Term term, NodeId vote);
  void AppendToLog(const std::vector<Entry>& entries);
  void ResetElectionTimer();
  [[nodiscard]] bool LogUpToDate(Index last_index, Term last_term) const;
  [[nodiscard]] size_t Quorum() const { return config_.peers.size() / 2 + 1; }
  [[noreturn]] static void Fatal(const Status& s);

  Config config_;
  LogStorage* storage_;
  std::mt19937_64 rng_;

  Role role_ = Role::kFollower;
  Term term_ = 0;
  NodeId vote_ = kNoNode;
  NodeId leader_ = kNoNode;
  Index commit_ = 0;
  Index applied_ = 0;

  int election_elapsed_ = 0;
  int heartbeat_elapsed_ = 0;
  int randomized_election_ticks_ = 0;

  std::map<NodeId, bool> votes_;  // this round's responses: granted or rejected
  std::map<NodeId, Progress> progress_;

  std::vector<Message> outbox_;
};

}  // namespace kv::raft

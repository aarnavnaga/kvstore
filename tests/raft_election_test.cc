#include <gtest/gtest.h>

#include <cstdio>
#include <random>

#include "raft/raft.h"
#include "raft_sim.h"
#include "test_util.h"

namespace kv::raft {
namespace {

Config ThreeNodeConfig(NodeId id) {
  Config c;
  c.id = id;
  c.peers = {1, 2, 3};
  c.seed = id;
  return c;
}

Message VoteRequest(NodeId from, NodeId to, Term term, Index last_index, Term last_term,
                    bool pre_vote = false) {
  Message m;
  m.kind = MsgKind::kVoteRequest;
  m.from = from;
  m.to = to;
  m.term = term;
  m.last_log_index = last_index;
  m.last_log_term = last_term;
  m.pre_vote = pre_vote;
  return m;
}

const Message* FindReply(const std::vector<Message>& out, MsgKind kind, NodeId to) {
  for (const auto& m : out) {
    if (m.kind == kind && m.to == to) {
      return &m;
    }
  }
  return nullptr;
}

// ---- unit tests: one node, hand-delivered messages -------------------------------------

TEST(RaftElection, SingleNodeElectsItself) {
  MemoryLogStorage storage;
  Config c;
  c.id = 1;
  c.peers = {1};
  RaftNode n(c, &storage);
  for (int i = 0; i < 25 && n.role() != Role::kLeader; ++i) {
    n.Tick();
  }
  EXPECT_EQ(n.role(), Role::kLeader);
  EXPECT_EQ(n.term(), 1u);
  EXPECT_EQ(n.commit_index(), 1u);  // its no-op commits with a quorum of one
}

TEST(RaftElection, VoteIsDurableAcrossRestart) {
  MemoryLogStorage storage;
  {
    RaftNode n(ThreeNodeConfig(1), &storage);
    n.Step(VoteRequest(2, 1, 5, 0, 0));
    const auto out = n.TakeMessages();
    const Message* r = FindReply(out, MsgKind::kVoteResponse, 2);
    ASSERT_NE(r, nullptr);
    EXPECT_TRUE(r->granted);
    // The vote reached storage before the reply was produced.
    EXPECT_EQ(storage.LoadHardState(), (HardState{5, 2}));
  }
  // Crash and restart from the same storage: a second candidate in term 5 is refused.
  RaftNode n(ThreeNodeConfig(1), &storage);
  EXPECT_EQ(n.term(), 5u);
  n.Step(VoteRequest(3, 1, 5, 0, 0));
  const auto out = n.TakeMessages();
  const Message* r = FindReply(out, MsgKind::kVoteResponse, 3);
  ASSERT_NE(r, nullptr);
  EXPECT_FALSE(r->granted);
  // The original candidate asking again still gets its vote.
  n.Step(VoteRequest(2, 1, 5, 0, 0));
  const auto again = n.TakeMessages();  // keep the vector alive while r points into it
  r = FindReply(again, MsgKind::kVoteResponse, 2);
  ASSERT_NE(r, nullptr);
  EXPECT_TRUE(r->granted);
}

TEST(RaftElection, RefusesCandidateWithStaleLog) {
  MemoryLogStorage storage;
  ASSERT_TRUE(storage.Append({Entry{1, 1}, Entry{2, 2}}).ok());
  ASSERT_TRUE(storage.SaveHardState({2, kNoNode}).ok());
  RaftNode n(ThreeNodeConfig(1), &storage);

  // Lower last term loses even with a longer log.
  n.Step(VoteRequest(2, 1, 3, 10, 1));
  auto out = n.TakeMessages();
  ASSERT_NE(FindReply(out, MsgKind::kVoteResponse, 2), nullptr);
  EXPECT_FALSE(FindReply(out, MsgKind::kVoteResponse, 2)->granted);

  // Same last term but shorter log loses too.
  n.Step(VoteRequest(3, 1, 4, 1, 2));
  out = n.TakeMessages();
  ASSERT_NE(FindReply(out, MsgKind::kVoteResponse, 3), nullptr);
  EXPECT_FALSE(FindReply(out, MsgKind::kVoteResponse, 3)->granted);

  // Equal log wins.
  n.Step(VoteRequest(3, 1, 5, 2, 2));
  out = n.TakeMessages();
  ASSERT_NE(FindReply(out, MsgKind::kVoteResponse, 3), nullptr);
  EXPECT_TRUE(FindReply(out, MsgKind::kVoteResponse, 3)->granted);
}

TEST(RaftElection, PreVoteDoesNotChangeTerm) {
  MemoryLogStorage storage;
  RaftNode n(ThreeNodeConfig(1), &storage);
  // An isolated node keeps timing out, but its PreVotes never get answered, so it never
  // bumps its term. Without PreVote it would be at term ~50 by now and depose the
  // leader the moment it reconnected.
  for (int i = 0; i < 1000; ++i) {
    n.Tick();
    (void)n.TakeMessages();
  }
  EXPECT_EQ(n.term(), 0u);
  EXPECT_EQ(n.role(), Role::kPreCandidate);
}

TEST(RaftElection, PreVoteRequestLeavesVoterUnchanged) {
  MemoryLogStorage storage;
  RaftNode n(ThreeNodeConfig(1), &storage);
  n.Step(VoteRequest(2, 1, 1, 0, 0, /*pre_vote=*/true));
  const auto out = n.TakeMessages();
  const Message* r = FindReply(out, MsgKind::kVoteResponse, 2);
  ASSERT_NE(r, nullptr);
  EXPECT_TRUE(r->granted);
  EXPECT_TRUE(r->pre_vote);
  EXPECT_EQ(n.term(), 0u);
  EXPECT_EQ(storage.LoadHardState().vote, kNoNode);
}

TEST(RaftElection, LeaderStepsDownWithoutQuorum) {
  MemoryLogStorage storage;
  Config c = ThreeNodeConfig(1);
  RaftNode n(c, &storage);
  n.Campaign();
  (void)n.TakeMessages();
  // Grant PreVotes, then real votes, by hand.
  Message pv;
  pv.kind = MsgKind::kVoteResponse;
  pv.from = 2;
  pv.to = 1;
  pv.term = 1;
  pv.pre_vote = true;
  pv.granted = true;
  n.Step(pv);
  (void)n.TakeMessages();
  Message v = pv;
  v.pre_vote = false;
  n.Step(v);
  (void)n.TakeMessages();
  ASSERT_EQ(n.role(), Role::kLeader);

  // No follower ever answers: within two election timeouts the leader gives up.
  for (int i = 0; i < 2 * c.election_ticks_min + 1; ++i) {
    n.Tick();
    (void)n.TakeMessages();
  }
  EXPECT_EQ(n.role(), Role::kFollower);
}

// ---- simulation ---------------------------------------------------------------------------

using sim::SimCluster;
using sim::SimOptions;

TEST(RaftSim, ElectsOneLeaderPromptly) {
  for (const int n : {3, 5}) {
    SimCluster c(SimOptions{.nodes = n, .seed = 42});
    ASSERT_TRUE(c.RunUntil([&] { return c.StableLeader() != kNoNode; }, 200)) << n << " nodes";
    EXPECT_TRUE(c.violation().empty()) << c.violation();
  }
}

// SPEC M4 acceptance: a new leader is elected after the old one is killed.
TEST(RaftSim, NewLeaderAfterLeaderIsKilled) {
  for (uint64_t seed = 1; seed <= 50; ++seed) {
    SimCluster c(SimOptions{.nodes = 5, .seed = seed});
    ASSERT_TRUE(c.RunUntil([&] { return c.StableLeader() != kNoNode; }, 300));
    const NodeId old_leader = c.StableLeader();
    const Term old_term = c.raft(old_leader)->term();
    c.Crash(old_leader);
    ASSERT_TRUE(c.RunUntil(
        [&] {
          const NodeId l = c.StableLeader();
          return l != kNoNode && c.raft(l)->term() > old_term;
        },
        300))
        << "seed " << seed << ": no new leader after killing node " << old_leader;
    EXPECT_TRUE(c.violation().empty()) << c.violation();

    // And the old leader rejoins as a follower without disrupting the new one.
    const NodeId new_leader = c.StableLeader();
    c.Restart(old_leader);
    c.Run(200);
    EXPECT_EQ(c.StableLeader(), new_leader) << "seed " << seed;
  }
}

TEST(RaftSim, RejoiningPartitionedNodeDoesNotDisruptLeader) {
  SimCluster c(SimOptions{.nodes = 3, .seed = 9});
  ASSERT_TRUE(c.RunUntil([&] { return c.StableLeader() != kNoNode; }, 200));
  const NodeId leader = c.StableLeader();
  const Term term = c.raft(leader)->term();
  const NodeId follower = leader == 1 ? 2 : 1;

  c.Isolate(follower);
  c.Run(2000);  // dozens of election timeouts alone
  c.Heal();
  c.Run(200);
  // PreVote kept the isolated node's term from inflating, so it rejoins quietly.
  EXPECT_EQ(c.StableLeader(), leader);
  EXPECT_EQ(c.raft(leader)->term(), term);
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

// SPEC M4 acceptance: exactly one leader per term across 1,000 randomized runs.
//
// Each run is a random 3- or 5-node cluster under a random schedule of crashes,
// restarts, partitions, message loss, duplication and delay. Election safety is checked
// after every message. At the end every fault is healed and a stable leader must emerge.
TEST(RaftSim, ElectionSafetyAcrossRandomizedRuns) {
  const int64_t runs = test::EnvInt("KV_RAFT_RUNS", 1000);
  const uint64_t base = test::EnvInt("KV_SEED", 0) != 0 ? test::TestSeed() : 1;
  uint64_t total_terms = 0;
  for (int64_t run = 0; run < runs; ++run) {
    const uint64_t seed = base + static_cast<uint64_t>(run);
    std::mt19937_64 rng(seed);
    SimOptions opts;
    opts.nodes = rng() % 2 == 0 ? 3 : 5;
    opts.seed = seed;
    SimCluster c(opts);

    for (int phase = 0; phase < 20; ++phase) {
      const NodeId victim = 1 + rng() % static_cast<uint64_t>(opts.nodes);
      switch (rng() % 7) {
        case 0:
          c.Crash(victim);
          break;
        case 1:
          for (const NodeId id : c.ids()) {
            c.Restart(id);
          }
          break;
        case 2: {
          std::set<NodeId> side;
          for (const NodeId id : c.ids()) {
            if (rng() % 2 == 0) {
              side.insert(id);
            }
          }
          c.Partition(side);
          break;
        }
        case 3:
          c.Heal();
          break;
        case 4:
          c.set_drop_rate(static_cast<double>(rng() % 40) / 100.0);
          c.set_dup_rate(static_cast<double>(rng() % 20) / 100.0);
          break;
        case 5:
          c.set_delay(1, 1 + static_cast<int>(rng() % 10));
          break;
        case 6:
          if (const NodeId l = c.StableLeader(); l != kNoNode) {
            c.Crash(l);  // kill the leader specifically
          }
          break;
      }
      // Between scheduled faults, nodes also crash and come back within a few ticks.
      // Fast restarts matter: a node that forgets its vote only causes two leaders if
      // it votes, crashes, restarts and votes again inside one election.
      const bool churn = rng() % 2 == 0;
      const int ticks = 50 + static_cast<int>(rng() % 150);
      for (int t = 0; t < ticks; ++t) {
        if (churn && rng() % 20 == 0) {
          const NodeId n = 1 + rng() % static_cast<uint64_t>(opts.nodes);
          c.alive(n) ? c.Crash(n) : c.Restart(n);
        }
        c.Tick();
      }
      ASSERT_TRUE(c.violation().empty()) << c.violation();
    }

    // Heal everything: a leader must emerge.
    c.Heal();
    c.set_drop_rate(0);
    c.set_dup_rate(0);
    c.set_delay(1, 3);
    for (const NodeId id : c.ids()) {
      c.Restart(id);
    }
    ASSERT_TRUE(c.RunUntil([&] { return c.StableLeader() != kNoNode; }, 1000))
        << "seed " << seed << ": no leader after healing";
    ASSERT_TRUE(c.violation().empty()) << c.violation();
    total_terms += c.terms_with_leader();
  }
  std::printf("RaftSim: %lld runs, %llu terms with a leader, 0 election-safety violations\n",
              static_cast<long long>(runs), static_cast<unsigned long long>(total_terms));
}

}  // namespace
}  // namespace kv::raft

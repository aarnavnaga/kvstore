// Snapshots, log compaction, wiped-node rejoin and ReadIndex, in the simulator.

#include <gtest/gtest.h>

#include <cstdio>
#include <random>
#include <set>

#include "raft_sim.h"
#include "test_util.h"

namespace kv::raft {
namespace {

using sim::SimCluster;
using sim::SimOptions;

bool ProposeWhenLeader(SimCluster& c, const std::string& data, int max_ticks = 1000) {
  for (int i = 0; i < max_ticks; ++i) {
    if (c.Propose(data)) {
      return true;
    }
    c.Tick();
  }
  return false;
}

bool Converged(SimCluster& c, size_t at_least) {
  std::optional<std::vector<std::string>> first;
  for (const NodeId id : c.ids()) {
    if (!c.alive(id)) {
      continue;
    }
    auto d = c.AppliedData(id);
    if (d.size() < at_least) {
      return false;
    }
    if (!first) {
      first = std::move(d);
    } else if (d != *first) {
      return false;
    }
  }
  return first.has_value();
}

SimOptions Compacting(int nodes, uint64_t seed) {
  SimOptions o;
  o.nodes = nodes;
  o.seed = seed;
  o.compact_every = 10;
  o.compact_keep = 2;
  return o;
}

TEST(RaftSnapshot, LaggingFollowerCatchesUpFromSnapshot) {
  SimCluster c(Compacting(3, 1));
  ASSERT_TRUE(ProposeWhenLeader(c, "first"));
  c.Run(50);
  const NodeId leader = c.StableLeader();
  const NodeId lagger = leader == 3 ? 2 : 3;
  c.Crash(lagger);
  for (int i = 0; i < 200; ++i) {
    ASSERT_TRUE(ProposeWhenLeader(c, "cmd" + std::to_string(i)));
    c.Run(1);
  }
  c.Run(50);
  // The leader's log no longer starts anywhere near where the lagger stopped.
  ASSERT_GT(c.raft(leader)->last_index(), 150u);
  c.Restart(lagger);
  ASSERT_TRUE(c.RunUntil([&] { return Converged(c, 201); }, 1000));
  EXPECT_GE(c.snapshots_installed(), 1u);
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

TEST(RaftSnapshot, MultiChunkTransferSurvivesLossAndReordering) {
  SimOptions o = Compacting(3, 2);
  o.config.snapshot_chunk_bytes = 16;  // hundreds of chunks
  SimCluster c(o);
  ASSERT_TRUE(ProposeWhenLeader(c, "first"));
  c.Run(50);
  const NodeId leader = c.StableLeader();
  const NodeId lagger = leader == 3 ? 2 : 3;
  c.Crash(lagger);
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(ProposeWhenLeader(c, "cmd" + std::to_string(i)));
  }
  c.Run(50);
  c.set_drop_rate(0.15);
  c.set_dup_rate(0.1);
  c.set_delay(1, 6);
  c.Restart(lagger);
  ASSERT_TRUE(c.RunUntil([&] { return Converged(c, 101); }, 20000));
  EXPECT_GE(c.snapshots_installed(), 1u);
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

// SPEC M6 acceptance: a node wiped of its data rejoins and catches up from a snapshot.
TEST(RaftSnapshot, WipedNodeRejoinsAndCatchesUpFromSnapshot) {
  SimCluster c(Compacting(3, 3));
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(ProposeWhenLeader(c, "cmd" + std::to_string(i)));
  }
  ASSERT_TRUE(c.RunUntil([&] { return Converged(c, 100); }, 1000));
  const NodeId leader = c.StableLeader();
  const NodeId victim = leader == 3 ? 2 : 3;
  c.WipeAndRestart(victim);
  EXPECT_FALSE(c.raft(victim)->joined());
  ASSERT_TRUE(ProposeWhenLeader(c, "after-wipe"));
  ASSERT_TRUE(c.RunUntil([&] { return Converged(c, 101); }, 2000)) << c.Dump();
  EXPECT_GE(c.snapshots_installed(), 1u);
  EXPECT_TRUE(c.raft(victim)->joined());
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

TEST(RaftSnapshot, JoiningNodeRefusesToVote) {
  MemoryLogStorage storage;
  Config cfg;
  cfg.id = 3;
  cfg.peers = {1, 2, 3};
  cfg.join = true;
  RaftNode n(cfg, &storage);
  Message m;
  m.kind = MsgKind::kVoteRequest;
  m.from = 1;
  m.to = 3;
  m.term = 7;
  n.Step(m);
  bool granted = false;
  for (const auto& r : n.TakeMessages()) {
    granted |= r.kind == MsgKind::kVoteResponse && r.granted;
  }
  EXPECT_FALSE(granted) << "a wiped node may have voted in term 7 before; it must not again";
  for (int i = 0; i < 500; ++i) {
    n.Tick();
  }
  EXPECT_EQ(n.role(), Role::kFollower) << "a joining node must never campaign";
}

TEST(RaftReadIndex, ReadCoversEverythingCommittedBefore) {
  SimCluster c(SimOptions{.nodes = 3, .seed = 4});
  Index idx = 0;
  ASSERT_TRUE(ProposeWhenLeader(c, "x"));
  ASSERT_TRUE(c.Propose("y", &idx));
  ASSERT_TRUE(c.RunUntil([&] { return c.raft(c.StableLeader())->commit_index() >= idx; }, 200));
  ASSERT_TRUE(c.StartRead(c.StableLeader(), 1));
  std::vector<SimCluster::CompletedRead> reads;
  ASSERT_TRUE(c.RunUntil(
      [&] {
        for (auto& r : c.TakeReads()) {
          reads.push_back(r);
        }
        return !reads.empty();
      },
      200));
  EXPECT_GE(reads[0].index, idx);
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

TEST(RaftReadIndex, LeaderCutOffFromMajorityNeverServesAStaleRead) {
  SimCluster c(SimOptions{.nodes = 5, .seed = 5});
  ASSERT_TRUE(ProposeWhenLeader(c, "v1"));
  c.Run(50);
  const NodeId old_leader = c.StableLeader();
  ASSERT_NE(old_leader, kNoNode);

  // Strand the leader with one follower; the other three elect a new leader and write.
  std::set<NodeId> minority{old_leader, old_leader == 1 ? 2u : 1u};
  c.Partition(minority);
  ASSERT_TRUE(c.StartRead(old_leader, 1));  // issued while it still believes it leads
  ASSERT_TRUE(c.RunUntil(
      [&] {
        const NodeId l = c.StableLeader();
        return l != kNoNode && minority.count(l) == 0;
      },
      500));
  ASSERT_TRUE(ProposeWhenLeader(c, "v2"));
  c.Run(300);
  for (const auto& r : c.TakeReads()) {
    EXPECT_NE(r.node, old_leader) << "stranded leader confirmed a read it could not confirm";
  }
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

// Random faults with writes, reads, aggressive compaction, multi-chunk snapshots and
// disk wipes. The simulator checks election safety, state-machine safety (including
// the contents of every installed snapshot) and that no read misses a committed write.
TEST(RaftSnapshot, RandomizedRunsWithCompactionWipesAndReads) {
  const int64_t runs = test::EnvInt("KV_RAFT_RUNS", 500);
  uint64_t snapshots = 0;
  uint64_t reads = 0;
  for (int64_t run = test::EnvInt("KV_RAFT_FIRST_RUN", 0); run < runs; ++run) {
    const uint64_t seed = 50'000 + static_cast<uint64_t>(run);
    std::mt19937_64 rng(seed);
    SimOptions opts = Compacting(rng() % 2 == 0 ? 3 : 5, seed);
    opts.compact_every = 5 + static_cast<int>(rng() % 20);
    // Small enough that every transfer spans many chunks. Stop-and-wait sends one chunk
    // per round trip, so much smaller chunks just make transfers take thousands of ticks.
    opts.config.snapshot_chunk_bytes = 64 + rng() % 1024;
    if (rng() % 2 == 0) {
      opts.config.max_append_bytes = 1;
    }
    SimCluster c(opts);
    int next_cmd = 0;
    uint64_t next_ctx = 1;

    for (int phase = 0; phase < 12; ++phase) {
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
          c.set_drop_rate(static_cast<double>(rng() % 25) / 100.0);
          c.set_dup_rate(static_cast<double>(rng() % 15) / 100.0);
          break;
        case 5:
          // Only one wiped-and-rejoining node at a time: two would be a majority of a
          // 3-node cluster with amnesia, which no consensus protocol survives.
          if (!c.AnyJoining()) {
            c.WipeAndRestart(victim);
          }
          break;
        case 6:
          if (const NodeId l = c.StableLeader(); l != kNoNode) {
            c.Crash(l);
          }
          break;
      }
      for (int t = 0; t < 120; ++t) {
        if (rng() % 3 == 0) {
          c.Propose("cmd" + std::to_string(next_cmd++));
        }
        if (rng() % 4 == 0) {
          const NodeId n = 1 + rng() % static_cast<uint64_t>(opts.nodes);
          c.StartRead(n, next_ctx++);
        }
        c.Tick();
      }
      reads += c.TakeReads().size();
      ASSERT_TRUE(c.violation().empty()) << c.violation();
    }

    c.Heal();
    c.set_drop_rate(0);
    c.set_dup_rate(0);
    c.set_delay(1, 3);
    for (const NodeId id : c.ids()) {
      c.Restart(id);
    }
    ASSERT_TRUE(c.CommitEventually("final", 8000)) << "seed " << seed << c.Dump();
    ASSERT_TRUE(c.RunUntil([&] { return Converged(c, 1); }, 5000))
        << "seed " << seed << ": no convergence" << c.Dump();
    ASSERT_TRUE(c.violation().empty()) << c.violation();
    snapshots += c.snapshots_installed();
  }
  std::printf(
      "RaftSnapshot: %lld runs, %llu snapshots installed, %llu reads checked, 0 violations\n",
      static_cast<long long>(runs), static_cast<unsigned long long>(snapshots),
      static_cast<unsigned long long>(reads));
}

}  // namespace
}  // namespace kv::raft

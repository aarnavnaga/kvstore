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

bool ProposeWhenLeader(SimCluster& c, const std::string& data, int max_ticks = 500) {
  for (int i = 0; i < max_ticks; ++i) {
    if (c.Propose(data)) {
      return true;
    }
    c.Tick();
  }
  return false;
}

// Converged reports whether every live node has applied the same entries.
bool Converged(SimCluster& c) {
  const std::vector<std::string>* first = nullptr;
  std::vector<std::vector<std::string>> all;
  for (const NodeId id : c.ids()) {
    if (c.alive(id)) {
      all.push_back(c.AppliedData(id));
    }
  }
  for (const auto& a : all) {
    if (first == nullptr) {
      first = &a;
    } else if (a != *first) {
      return false;
    }
  }
  return true;
}

// Describe summarizes each node for a failure message.
std::string Describe(SimCluster& c) {
  std::string out;
  for (const NodeId id : c.ids()) {
    out += "\n  node " + std::to_string(id) + ": ";
    if (RaftNode* r = c.raft(id)) {
      out += std::string(RoleName(r->role())) + " term=" + std::to_string(r->term()) +
             " commit=" + std::to_string(r->commit_index()) +
             " last=" + std::to_string(r->last_index());
    } else {
      out += "down";
    }
    const auto d = c.AppliedData(id);
    out += " applied=" + std::to_string(d.size()) + (d.empty() ? "" : " tail=" + d.back());
  }
  if (!c.violation().empty()) {
    out += "\n  violation: " + c.violation();
  }
  return out;
}

TEST(RaftReplication, EveryNodeAppliesTheSameLogInOrder) {
  SimCluster c(SimOptions{.nodes = 5, .seed = 3});
  for (int i = 0; i < 300; ++i) {
    ASSERT_TRUE(ProposeWhenLeader(c, "cmd" + std::to_string(i)));
    c.Run(1);
  }
  ASSERT_TRUE(c.RunUntil([&] { return Converged(c) && c.AppliedData(1).size() == 300; }, 500));
  const auto applied = c.AppliedData(1);
  for (int i = 0; i < 300; ++i) {
    EXPECT_EQ(applied[i], "cmd" + std::to_string(i));
  }
  EXPECT_TRUE(c.violation().empty()) << c.violation();
}

// SPEC M5 acceptance: writes survive the loss of any minority of nodes.
TEST(RaftReplication, WritesSurviveLossOfAnyMinority) {
  for (const int n : {3, 5}) {
    const int minority = (n - 1) / 2;
    // Every subset of `minority` nodes, including those that contain the leader.
    std::vector<std::set<NodeId>> subsets;
    for (uint32_t mask = 0; mask < (1u << n); ++mask) {
      if (__builtin_popcount(mask) == minority) {
        std::set<NodeId> s;
        for (int i = 0; i < n; ++i) {
          if ((mask & (1u << i)) != 0) {
            s.insert(static_cast<NodeId>(i + 1));
          }
        }
        subsets.push_back(s);
      }
    }
    for (const auto& lost : subsets) {
      SimCluster c(SimOptions{.nodes = n, .seed = 100 + lost.size() + *lost.begin()});
      for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(ProposeWhenLeader(c, "before" + std::to_string(i)));
      }
      ASSERT_TRUE(c.RunUntil([&] { return Converged(c) && c.AppliedData(1).size() == 50; }, 500));

      for (const NodeId id : lost) {
        c.Crash(id);
      }
      for (int i = 0; i < 50; ++i) {
        ASSERT_TRUE(ProposeWhenLeader(c, "during" + std::to_string(i), 1000))
            << n << " nodes, lost " << lost.size() << ": no leader for write " << i;
      }
      for (const NodeId id : lost) {
        c.Restart(id);
      }
      ASSERT_TRUE(c.RunUntil([&] { return Converged(c) && c.AppliedData(1).size() >= 100; }, 2000));
      // Nothing written before the loss went missing, on any node.
      for (const NodeId id : c.ids()) {
        const auto data = c.AppliedData(id);
        const std::set<std::string> have(data.begin(), data.end());
        for (int i = 0; i < 50; ++i) {
          ASSERT_TRUE(have.count("before" + std::to_string(i))) << "node " << id;
          ASSERT_TRUE(have.count("during" + std::to_string(i))) << "node " << id;
        }
      }
      ASSERT_TRUE(c.violation().empty()) << c.violation();
    }
  }
}

// Random faults with a steady stream of writes. The simulator checks state-machine
// safety on every apply; after healing, every node must converge on one log that holds
// every write any node ever applied.
TEST(RaftReplication, RandomizedFaultsConverge) {
  const int64_t runs = test::EnvInt("KV_RAFT_RUNS", 500);
  uint64_t total_applied = 0;
  for (int64_t run = 0; run < runs; ++run) {
    const uint64_t seed = 10'000 + static_cast<uint64_t>(run);
    std::mt19937_64 rng(seed);
    SimOptions opts;
    opts.nodes = rng() % 2 == 0 ? 3 : 5;
    opts.seed = seed;
    // Half the runs replicate one entry per message. Bugs that only bite when a
    // follower receives part of the leader's log (e.g. committing past what the leader
    // vouched for) are invisible when every append carries the whole log.
    if (rng() % 2 == 0) {
      opts.config.max_append_bytes = 1;
    }
    SimCluster c(opts);
    int next_cmd = 0;

    for (int phase = 0; phase < 15; ++phase) {
      const NodeId victim = 1 + rng() % static_cast<uint64_t>(opts.nodes);
      switch (rng() % 7) {
        case 6:
          // Kill the leader specifically: leadership changes while entries are only
          // partly replicated are where commit-rule bugs (Raft paper, Figure 8) live.
          if (const NodeId l = c.StableLeader(); l != kNoNode) {
            c.Crash(l);
          }
          break;
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
          c.set_drop_rate(static_cast<double>(rng() % 30) / 100.0);
          c.set_dup_rate(static_cast<double>(rng() % 20) / 100.0);
          break;
        case 5:
          c.set_delay(1, 1 + static_cast<int>(rng() % 8));
          break;
      }
      for (int t = 0; t < 100; ++t) {
        if (rng() % 3 == 0) {
          c.Propose("cmd" + std::to_string(next_cmd++));
        }
        if (rng() % 15 == 0) {
          const NodeId n = 1 + rng() % static_cast<uint64_t>(opts.nodes);
          c.alive(n) ? c.Crash(n) : c.Restart(n);
        }
        c.Tick();
      }
      ASSERT_TRUE(c.violation().empty()) << c.violation();
    }

    c.Heal();
    c.set_drop_rate(0);
    c.set_dup_rate(0);
    c.set_delay(1, 3);
    for (const NodeId id : c.ids()) {
      c.Restart(id);
    }
    // One more write forces everything earlier to commit everywhere.
    ASSERT_TRUE(ProposeWhenLeader(c, "final", 2000)) << "seed " << seed;
    ASSERT_TRUE(c.RunUntil(
        [&] {
          if (!Converged(c)) {
            return false;
          }
          const auto d = c.AppliedData(1);
          return !d.empty() && d.back() == "final";
        },
        3000))
        << "seed " << seed << ": nodes did not converge after healing" << Describe(c);
    ASSERT_TRUE(c.violation().empty()) << c.violation();
    total_applied += c.AppliedData(1).size();
  }
  std::printf("RaftReplication: %lld runs, %llu entries applied on every node, 0 violations\n",
              static_cast<long long>(runs), static_cast<unsigned long long>(total_applied));
}

}  // namespace
}  // namespace kv::raft

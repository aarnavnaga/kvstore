// End-to-end tests against real replicas over TCP: replication, redirects, leader
// failure during writes, loss of a minority, and full-cluster restarts.

#include <gtest/gtest.h>

#include <atomic>
#include <mutex>
#include <set>
#include <thread>

#include "client/kv_client.h"
#include "cluster_util.h"

namespace kv {
namespace {

using namespace std::chrono_literals;

client::ClientOptions ClientFor(const std::vector<std::string>& servers) {
  client::ClientOptions o;
  o.servers = servers;
  o.request_timeout = 2s;
  o.operation_timeout = 15s;
  return o;
}

// Every key in want must be readable through the cluster with its value.
void ExpectAllReadable(const std::vector<std::string>& servers,
                       const std::map<std::string, std::string>& want) {
  client::KvClient c(ClientFor(servers));
  for (const auto& [k, v] : want) {
    std::string got;
    const Status s = c.Get(k, &got);
    ASSERT_TRUE(s.ok()) << k << ": " << s.ToString();
    ASSERT_EQ(got, v) << k;
  }
}

TEST(Cluster, ReplicatesToEveryNodeInTheSameOrder) {
  test::TestCluster cluster(3);
  ASSERT_NE(cluster.WaitForLeader(), raft::kNoNode);
  client::KvClient c(ClientFor(cluster.addrs()));
  std::map<std::string, std::string> want;
  for (int i = 0; i < 300; ++i) {
    const std::string k = "key" + std::to_string(i);
    want[k] = "value" + std::to_string(i);
    ASSERT_TRUE(c.Put(k, want[k]).ok());
  }
  for (int i = 0; i < 300; i += 3) {
    const std::string k = "key" + std::to_string(i);
    ASSERT_TRUE(c.Delete(k).ok());
    want.erase(k);
  }
  ASSERT_TRUE(cluster.WaitForConvergence());

  // Same applied index and hash everywhere, and each node's own LSM tree holds the data.
  for (const auto id : cluster.ids()) {
    for (const auto& [k, v] : want) {
      std::string got;
      ASSERT_TRUE(cluster.state_machine(id)->Get(k, &got).ok()) << "node " << id << " " << k;
      ASSERT_EQ(got, v);
    }
    std::string got;
    EXPECT_TRUE(cluster.state_machine(id)->Get("key0", &got).IsNotFound());
  }
}

TEST(Cluster, FollowerRedirectsClientToLeader) {
  test::TestCluster cluster(3);
  const raft::NodeId leader = cluster.WaitForLeader();
  ASSERT_NE(leader, raft::kNoNode);
  const raft::NodeId follower = leader == 1 ? 2 : 1;

  // The client only knows a follower; the NotLeader hint takes it to the leader.
  client::KvClient c(ClientFor({cluster.addr(follower)}));
  ASSERT_TRUE(c.Put("k", "v").ok());
  EXPECT_EQ(c.leader(), cluster.addr(leader));
  std::string v;
  ASSERT_TRUE(c.Get("k", &v).ok());
  EXPECT_EQ(v, "v");
}

TEST(Cluster, AcknowledgedWritesSurviveLeaderFailure) {
  test::TestCluster cluster(3);
  const raft::NodeId leader = cluster.WaitForLeader();
  ASSERT_NE(leader, raft::kNoNode);

  std::mutex mu;
  std::map<std::string, std::string> acked;
  std::atomic<bool> stop{false};
  std::atomic<int> unknown{0};
  std::vector<std::thread> writers;
  for (int w = 0; w < 4; ++w) {
    writers.emplace_back([&, w] {
      client::KvClient c(ClientFor(cluster.addrs()));
      for (int i = 0; !stop; ++i) {
        const std::string k = "w" + std::to_string(w) + "-" + std::to_string(i);
        const Status s = c.Put(k, k);
        if (s.ok()) {
          std::lock_guard lk(mu);
          acked[k] = k;
        } else {
          ++unknown;  // timed out: may or may not have committed, so not checked
        }
      }
    });
  }
  std::this_thread::sleep_for(500ms);
  cluster.Stop(leader);  // the leader goes away mid-stream
  const raft::NodeId new_leader = cluster.WaitForLeader();
  ASSERT_NE(new_leader, raft::kNoNode);
  EXPECT_NE(new_leader, leader);
  std::this_thread::sleep_for(700ms);
  cluster.Restart(leader);  // and comes back as a follower
  std::this_thread::sleep_for(500ms);
  stop = true;
  for (auto& t : writers) {
    t.join();
  }

  ASSERT_GT(acked.size(), 100u);
  ExpectAllReadable(cluster.addrs(), acked);
  ASSERT_TRUE(cluster.WaitForConvergence());
  std::printf("leader failover: %zu acknowledged writes verified, %d outcomes unknown\n",
              acked.size(), unknown.load());
}

// SPEC M5 acceptance, on the real system: writes survive the loss of any minority.
TEST(Cluster, WritesSurviveLossOfMinority) {
  test::TestCluster cluster(5);
  const raft::NodeId leader = cluster.WaitForLeader();
  ASSERT_NE(leader, raft::kNoNode);
  client::KvClient c(ClientFor(cluster.addrs()));
  std::map<std::string, std::string> want;
  for (int i = 0; i < 100; ++i) {
    want["a" + std::to_string(i)] = "1";
    ASSERT_TRUE(c.Put("a" + std::to_string(i), "1").ok());
  }

  // Lose two of five, one of them the leader.
  const raft::NodeId other = leader == 5 ? 4 : 5;
  cluster.Stop(leader);
  cluster.Stop(other);
  for (int i = 0; i < 100; ++i) {
    want["b" + std::to_string(i)] = "2";
    ASSERT_TRUE(c.Put("b" + std::to_string(i), "2").ok()) << "write " << i;
  }
  ExpectAllReadable(cluster.addrs(), want);

  // Both come back and catch up.
  cluster.Restart(leader);
  cluster.Restart(other);
  ASSERT_TRUE(c.Put("final", "3").ok());
  want["final"] = "3";
  ASSERT_TRUE(cluster.WaitForConvergence());
  for (const auto id : {leader, other}) {
    for (const auto& [k, v] : want) {
      std::string got;
      ASSERT_TRUE(cluster.state_machine(id)->Get(k, &got).ok()) << "node " << id << " " << k;
    }
  }
}

TEST(Cluster, FullRestartKeepsEverything) {
  test::TestCluster cluster(3);
  ASSERT_NE(cluster.WaitForLeader(), raft::kNoNode);
  std::map<std::string, std::string> want;
  {
    client::KvClient c(ClientFor(cluster.addrs()));
    for (int i = 0; i < 200; ++i) {
      want["k" + std::to_string(i)] = std::string(100, static_cast<char>('a' + i % 26));
      ASSERT_TRUE(c.Put("k" + std::to_string(i), want["k" + std::to_string(i)]).ok());
    }
  }
  for (const auto id : cluster.ids()) {
    cluster.Stop(id);
  }
  for (const auto id : cluster.ids()) {
    cluster.Restart(id);
  }
  ASSERT_NE(cluster.WaitForLeader(), raft::kNoNode);
  ExpectAllReadable(cluster.addrs(), want);
}

}  // namespace
}  // namespace kv

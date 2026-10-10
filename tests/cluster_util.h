#pragma once

// TestCluster runs N real replicas in one process: each with its own epoll server on a
// loopback port, its own data directory, a FileLogStorage and an LSM state machine, all
// talking over real TCP. Nodes can be stopped and restarted on the same port and data.

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "common/file.h"
#include "net/server.h"
#include "server/replica.h"
#include "test_util.h"

namespace kv::test {

class TestCluster {
 public:
  struct Options {
    int tick_ms = 10;
    uint64_t snapshot_threshold = 10000;
    uint64_t snapshot_keep = 1000;
    bool read_through_log = false;
    std::shared_ptr<net::TlsContext> tls;  // mutual TLS on every connection
  };

  explicit TestCluster(int n) : TestCluster(n, Options{}) {}
  TestCluster(int n, Options options) : options_(options) {
    // Bind every server first (port 0) so each node knows all addresses up front.
    for (int i = 1; i <= n; ++i) {
      auto& node = nodes_[static_cast<raft::NodeId>(i)];
      node.holder = std::make_shared<Holder>();
      StartServer(static_cast<raft::NodeId>(i), 0);
      peers_[static_cast<raft::NodeId>(i)] = "127.0.0.1:" + std::to_string(node.port);
    }
    for (int i = 1; i <= n; ++i) {
      StartReplica(static_cast<raft::NodeId>(i));
    }
  }

  ~TestCluster() {
    for (auto& [id, node] : nodes_) {
      Stop(id);
    }
  }

  void Stop(raft::NodeId id) {
    Node& node = nodes_[id];
    if (node.server) {
      node.server->Stop();
      node.server.reset();
    }
    {
      std::lock_guard lk(node.holder->mu);
      node.holder->replica = nullptr;
    }
    node.replica.reset();
  }

  void Restart(raft::NodeId id) {
    Stop(id);
    StartServer(id, nodes_[id].port);
    StartReplica(id);
  }

  // Wipe deletes the node's data directory and restarts it in join mode, as an operator
  // would after replacing a failed disk.
  void Wipe(raft::NodeId id) {
    Stop(id);
    (void)RemoveDirRecursive(DataDir(id));
    StartServer(id, nodes_[id].port);
    StartReplica(id, /*join=*/true);
  }

  [[nodiscard]] bool running(raft::NodeId id) const { return nodes_.at(id).replica != nullptr; }

  server::ReplicaStatus status(raft::NodeId id) const { return nodes_.at(id).replica->status(); }

  server::KvStateMachine* state_machine(raft::NodeId id) {
    return nodes_.at(id).replica->state_machine();
  }

  [[nodiscard]] std::vector<std::string> addrs() const {
    std::vector<std::string> out;
    for (const auto& [id, addr] : peers_) {
      out.push_back(addr);
    }
    return out;
  }
  [[nodiscard]] std::string addr(raft::NodeId id) const { return peers_.at(id); }
  [[nodiscard]] std::vector<raft::NodeId> ids() const {
    std::vector<raft::NodeId> out;
    for (const auto& [id, addr] : peers_) {
      out.push_back(id);
    }
    return out;
  }

  // WaitForLeader returns the leader a majority of running nodes agree on.
  raft::NodeId WaitForLeader(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      std::map<std::pair<raft::Term, raft::NodeId>, size_t> votes;
      for (const auto& [id, node] : nodes_) {
        if (node.replica) {
          const auto s = node.replica->status();
          if (s.leader != raft::kNoNode) {
            ++votes[{s.term, s.leader}];
          }
        }
      }
      for (const auto& [key, count] : votes) {
        if (count >= nodes_.size() / 2 + 1 && running(key.second) &&
            status(key.second).role == raft::Role::kLeader) {
          return key.second;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return raft::kNoNode;
  }

  // WaitForConvergence waits until every running node has applied the same log.
  bool WaitForConvergence(std::chrono::milliseconds timeout = std::chrono::seconds(15)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      bool same = true;
      std::optional<server::ReplicaStatus> first;
      for (const auto& [id, node] : nodes_) {
        if (!node.replica) {
          continue;
        }
        const auto s = node.replica->status();
        if (!first) {
          first = s;
        } else if (s.applied != first->applied || s.apply_hash != first->apply_hash) {
          same = false;
        }
      }
      if (same && first && first->applied > 0 && first->applied == first->commit) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
  }

 private:
  struct Holder {
    std::mutex mu;
    server::Replica* replica = nullptr;
  };
  struct Node {
    uint16_t port = 0;
    std::shared_ptr<Holder> holder;
    std::unique_ptr<net::Server> server;
    std::unique_ptr<server::Replica> replica;
  };

  void StartServer(raft::NodeId id, uint16_t port) {
    Node& node = nodes_[id];
    std::shared_ptr<Holder> holder = node.holder;
    net::ServerOptions opts;
    opts.host = "127.0.0.1";
    opts.port = port;
    opts.io_threads = 1;
    opts.worker_threads = 2;
    opts.tls = options_.tls;
    node.server =
        std::make_unique<net::Server>(opts, [holder](const net::Frame& f, const net::Responder& r) {
          std::lock_guard lk(holder->mu);
          if (holder->replica != nullptr) {
            holder->replica->Handle(f, r);
          }
        });
    // A just-closed listener's port can take a moment to become bindable again.
    for (int attempt = 0; attempt < 100; ++attempt) {
      if (node.server->Start().ok()) {
        node.port = node.server->port();
        return;
      }
      node.server = std::make_unique<net::Server>(
          opts, [holder](const net::Frame& f, const net::Responder& r) {
            std::lock_guard lk(holder->mu);
            if (holder->replica != nullptr) {
              holder->replica->Handle(f, r);
            }
          });
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    std::abort();
  }

  std::string DataDir(raft::NodeId id) const {
    return JoinPath(dir_.path(), "node" + std::to_string(id));
  }

  void StartReplica(raft::NodeId id, bool join = false) {
    Node& node = nodes_[id];
    server::ReplicaOptions o;
    o.id = id;
    o.peers = peers_;
    o.data_dir = DataDir(id);
    o.tick_ms = options_.tick_ms;
    o.snapshot_threshold = options_.snapshot_threshold;
    o.snapshot_keep = options_.snapshot_keep;
    o.read_through_log = options_.read_through_log;
    o.join = join;
    o.db.sync = false;
    o.tls = options_.tls;
    if (!server::Replica::Open(o, &node.replica).ok()) {
      std::abort();
    }
    node.replica->Start();
    std::lock_guard lk(node.holder->mu);
    node.holder->replica = node.replica.get();
  }

  Options options_;
  TempDir dir_;
  std::map<raft::NodeId, std::string> peers_;
  std::map<raft::NodeId, Node> nodes_;
};

}  // namespace kv::test

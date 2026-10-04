#pragma once

// Deterministic Raft simulator: N RaftNodes in one process, a logical clock, and a
// network that delays, drops, duplicates and reorders messages and can be partitioned,
// all driven by one seed. A failing seed replays exactly, which turns a one-in-a-million
// interleaving into a reproducible unit test.
//
// Invariants are checked continuously:
//   Election safety     at most one leader per term, ever
//   State machine safety no two nodes apply different entries at the same index
//   Apply order         every node applies a contiguous prefix of the log
//   Commit bound        no node's commit index passes the end of its own log

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "raft/raft.h"
#include "raft/storage.h"

namespace kv::raft::sim {

struct SimOptions {
  int nodes = 3;
  uint64_t seed = 1;
  int min_delay = 1;  // ticks a message spends in flight
  int max_delay = 3;
  double drop_rate = 0;
  double dup_rate = 0;
  // Log compaction: once a node has applied compact_every entries past its last
  // snapshot, it compacts its log, keeping compact_keep entries for slow followers.
  // 0 disables compaction.
  int compact_every = 0;
  int compact_keep = 2;
  Config config;  // template; id, peers and seed are filled per node
};

class SimCluster {
 public:
  explicit SimCluster(SimOptions options) : options_(std::move(options)), rng_(options_.seed) {
    if (const char* t = std::getenv("KV_SIM_TRACE_NODE")) {
      trace_node_ = std::strtoull(t, nullptr, 10);
    }
    if (const char* t = std::getenv("KV_SIM_TRACE_FROM")) {
      trace_from_ = std::strtoull(t, nullptr, 10);
    }
    for (int i = 1; i <= options_.nodes; ++i) {
      ids_.push_back(static_cast<NodeId>(i));
    }
    for (const NodeId id : ids_) {
      auto& n = nodes_[id];
      n.storage = std::make_unique<MemoryLogStorage>();
      Start(id);
    }
  }

  // ---- clock ------------------------------------------------------------------------

  void Tick() {
    ++now_;
    DeliverDue();
    for (const NodeId id : ids_) {
      if (auto* r = raft(id)) {
        r->Tick();
        Collect(id);
      }
    }
  }

  void Run(int ticks) {
    for (int i = 0; i < ticks && violation_.empty(); ++i) {
      Tick();
    }
  }

  // RunUntil ticks until pred holds, returning false if max_ticks pass first.
  bool RunUntil(const std::function<bool()>& pred, int max_ticks) {
    for (int i = 0; i < max_ticks; ++i) {
      if (pred()) {
        return true;
      }
      if (!violation_.empty()) {
        return false;
      }
      Tick();
    }
    return pred();
  }

  // ---- faults -----------------------------------------------------------------------

  // Crash loses everything except the node's durable storage and applied state.
  void Crash(NodeId id) { nodes_[id].raft.reset(); }

  void Restart(NodeId id) {
    if (!nodes_[id].raft) {
      Start(id);
    }
  }

  // WipeAndRestart loses the node's durable state as well (a replaced disk). It comes
  // back in join mode, as an operator would restart it.
  void WipeAndRestart(NodeId id) {
    auto& n = nodes_[id];
    n.raft.reset();
    n.storage = std::make_unique<MemoryLogStorage>();
    n.applied.clear();
    Start(id, /*join=*/true);
  }

  [[nodiscard]] bool alive(NodeId id) const { return nodes_.at(id).raft != nullptr; }

  // AnyJoining reports whether a wiped node is still catching up, counting crashed ones
  // through their durable joining flag.
  [[nodiscard]] bool AnyJoining() const {
    for (const auto& [id, n] : nodes_) {
      if (n.raft ? !n.raft->joined() : n.storage->LoadHardState().joining) {
        return true;
      }
    }
    return false;
  }

  // CommitEventually keeps proposing marker writes (as a client retries) until one of
  // them is applied on every live node. A proposal can legitimately vanish if its
  // leader loses leadership before replicating it; only committed writes are durable.
  bool CommitEventually(const std::string& prefix, int max_ticks) {
    int attempt = 0;
    std::string marker;
    for (int t = 0; t < max_ticks; ++t) {
      if (t % 300 == 0) {
        const std::string next = prefix + std::to_string(attempt);
        if (Propose(next)) {
          marker = next;
          ++attempt;
        }
      }
      bool everywhere = !marker.empty();
      for (const NodeId id : ids_) {
        if (!everywhere) {
          break;
        }
        if (raft(id) == nullptr) {
          continue;
        }
        const auto d = AppliedData(id);
        everywhere = !d.empty() && d.back().rfind(prefix, 0) == 0;
      }
      if (everywhere) {
        return true;
      }
      if (!violation_.empty()) {
        return false;
      }
      Tick();
    }
    return false;
  }

  // Partition splits the cluster: side and everyone else can no longer talk.
  void Partition(const std::set<NodeId>& side) {
    for (const NodeId id : ids_) {
      nodes_[id].group = side.count(id) != 0 ? 1 : 0;
    }
  }
  void Isolate(NodeId id) { Partition({id}); }
  void Heal() {
    for (const NodeId id : ids_) {
      nodes_[id].group = 0;
    }
  }

  void set_drop_rate(double p) { options_.drop_rate = p; }
  void set_dup_rate(double p) { options_.dup_rate = p; }
  void set_delay(int min, int max) {
    options_.min_delay = min;
    options_.max_delay = max;
  }

  // ---- queries ------------------------------------------------------------------------

  RaftNode* raft(NodeId id) { return nodes_[id].raft.get(); }
  [[nodiscard]] const std::vector<NodeId>& ids() const { return ids_; }
  [[nodiscard]] uint64_t now() const { return now_; }
  [[nodiscard]] const std::string& violation() const { return violation_; }
  [[nodiscard]] const std::vector<Entry>& applied(NodeId id) const { return nodes_.at(id).applied; }
  [[nodiscard]] size_t terms_with_leader() const { return leaders_by_term_.size(); }
  [[nodiscard]] uint64_t snapshots_installed() const { return snapshots_installed_; }

  // StableLeader returns a leader that a quorum of nodes (itself included) currently
  // agree on, at the same term, or kNoNode.
  NodeId StableLeader() {
    for (const NodeId id : ids_) {
      RaftNode* r = raft(id);
      if (r == nullptr || r->role() != Role::kLeader) {
        continue;
      }
      size_t agree = 0;
      for (const NodeId other : ids_) {
        RaftNode* o = raft(other);
        if (o != nullptr && o->term() == r->term() && o->leader() == id) {
          ++agree;
        }
      }
      if (agree >= ids_.size() / 2 + 1) {
        return id;
      }
    }
    return kNoNode;
  }

  // Propose submits data to the stable leader. Returns false if there is none.
  bool Propose(const std::string& data, Index* index = nullptr) {
    const NodeId leader = StableLeader();
    if (leader == kNoNode) {
      return false;
    }
    Index i = 0;
    Term t = 0;
    if (!raft(leader)->Propose(data, &i, &t).ok()) {
      return false;
    }
    if (index != nullptr) {
      *index = i;
    }
    Collect(leader);
    return true;
  }

  // Dump summarizes every node, for failure messages.
  std::string Dump() {
    std::ostringstream os;
    for (const NodeId id : ids_) {
      os << "\n  node " << id << ": ";
      if (RaftNode* r = raft(id)) {
        os << RoleName(r->role()) << " term=" << r->term() << " leader=" << r->leader()
           << " commit=" << r->commit_index() << " applied=" << r->applied_index()
           << " last=" << r->last_index() << " snap=" << nodes_[id].storage->Snapshot().index
           << " joined=" << r->joined() << " installed=" << r->snapshots_installed()
           << " sent=" << r->snapshots_sent() << r->DebugString();
      } else {
        os << "down";
      }
      os << " sm_applied=" << nodes_[id].applied.size();
    }
    if (!violation_.empty()) {
      os << "\n  violation: " << violation_;
    }
    return os.str();
  }

  // StartRead issues a ReadIndex on node id. It records the highest commit index any node
  // has reached right now: a linearizable read must reflect at least that much.
  bool StartRead(NodeId id, uint64_t ctx) {
    RaftNode* r = raft(id);
    if (r == nullptr || !r->ReadIndex(ctx).ok()) {
      return false;
    }
    Index max_commit = 0;
    for (const NodeId n : ids_) {
      if (RaftNode* o = raft(n)) {
        max_commit = std::max(max_commit, o->commit_index());
      }
    }
    read_floor_[ctx] = max_commit;
    Collect(id);
    return true;
  }

  // TakeReads returns reads that completed (node, ctx, read index).
  struct CompletedRead {
    NodeId node;
    uint64_t ctx;
    Index index;
  };
  std::vector<CompletedRead> TakeReads() {
    std::vector<CompletedRead> out;
    out.swap(completed_reads_);
    return out;
  }

  // AppliedData returns the data of the normal entries node id has applied.
  std::vector<std::string> AppliedData(NodeId id) const {
    std::vector<std::string> out;
    for (const Entry& e : nodes_.at(id).applied) {
      if (e.type == EntryType::kNormal) {
        out.push_back(e.data);
      }
    }
    return out;
  }

 private:
  struct SimNode {
    std::unique_ptr<MemoryLogStorage> storage;
    std::unique_ptr<RaftNode> raft;
    std::vector<Entry> applied;  // durable, like the real state machine
    int group = 0;
  };
  struct InFlight {
    uint64_t deliver_at;
    Message m;
  };

  void Start(NodeId id, bool join = false) {
    auto& n = nodes_[id];
    Config c = options_.config;
    c.id = id;
    c.peers = ids_;
    c.seed = options_.seed * 7919 + id + 1000 * restarts_++;
    c.applied = n.applied.empty() ? n.storage->Snapshot().index : n.applied.back().index;
    c.join = join;
    // The simulated state machine is the list of applied entries; its snapshot is that
    // list, serialized.
    c.snapshot_source = [this, id] {
      const auto& applied = nodes_[id].applied;
      auto snap = std::make_shared<Snapshot>();
      if (applied.empty()) {
        return std::shared_ptr<const Snapshot>();
      }
      snap->meta = {applied.back().index, applied.back().term};
      for (const Entry& e : applied) {
        snap->data += EncodeEntry(e);
      }
      return std::shared_ptr<const Snapshot>(std::move(snap));
    };
    c.snapshot_sink = [this, id](const Snapshot& snap) {
      std::vector<Entry> entries;
      std::string_view in = snap.data;
      while (!in.empty()) {
        Entry e;
        if (!DecodeEntry(&in, &e)) {
          return Status::Corruption("bad simulated snapshot");
        }
        entries.push_back(std::move(e));
      }
      for (const Entry& e : entries) {
        CheckCommitted(id, e);  // a snapshot may only contain committed history
      }
      nodes_[id].applied = std::move(entries);
      ++snapshots_installed_;
      return Status::OK();
    };
    n.raft = std::make_unique<RaftNode>(c, n.storage.get());
  }

  bool Connected(NodeId a, NodeId b) const { return nodes_.at(a).group == nodes_.at(b).group; }

  void Collect(NodeId id) {
    RaftNode* r = raft(id);
    for (Message& m : r->TakeMessages()) {
      Send(std::move(m));
    }
    if (r->commit_index() > r->last_index()) {
      Fail("node " + std::to_string(id) + " committed through index " +
           std::to_string(r->commit_index()) + " but its log ends at " +
           std::to_string(r->last_index()));
      return;
    }
    for (const auto& rs : r->TakeReadStates()) {
      const auto it = read_floor_.find(rs.ctx);
      if (it != read_floor_.end() && rs.index < it->second) {
        Fail("stale read: ctx " + std::to_string(rs.ctx) + " on node " + std::to_string(id) +
             " got read index " + std::to_string(rs.index) + " but index " +
             std::to_string(it->second) + " was already committed when it was issued");
        return;
      }
      completed_reads_.push_back({id, rs.ctx, rs.index});
    }
    ApplyCommitted(id);
    CheckLeader(id);
  }

  void Send(Message m) {
    std::uniform_real_distribution<double> coin(0, 1);
    if (!Connected(m.from, m.to) || coin(rng_) < options_.drop_rate) {
      return;
    }
    std::uniform_int_distribution<int> delay(options_.min_delay, options_.max_delay);
    if (coin(rng_) < options_.dup_rate) {
      in_flight_.push_back({now_ + static_cast<uint64_t>(delay(rng_)), m});
    }
    in_flight_.push_back({now_ + static_cast<uint64_t>(delay(rng_)), std::move(m)});
  }

  void DeliverDue() {
    std::vector<InFlight> due;
    std::vector<InFlight> later;
    for (auto& f : in_flight_) {
      (f.deliver_at <= now_ ? due : later).push_back(std::move(f));
    }
    in_flight_.swap(later);
    // Messages due in the same tick arrive in random order.
    std::shuffle(due.begin(), due.end(), rng_);
    for (auto& f : due) {
      // A partition that formed while the message was in flight still cuts it.
      if (!Connected(f.m.from, f.m.to)) {
        continue;
      }
      if (RaftNode* r = raft(f.m.to)) {
        if (trace_node_ != kNoNode && (f.m.to == trace_node_ || f.m.from == trace_node_) &&
            now_ >= trace_from_) {
          std::fprintf(
              stderr,
              "t=%llu %llu->%llu %s term=%llu snap=%llu off=%llu len=%zu done=%d ok=%d "
              "match=%llu\n",
              static_cast<unsigned long long>(now_), static_cast<unsigned long long>(f.m.from),
              static_cast<unsigned long long>(f.m.to), MsgKindName(f.m.kind),
              static_cast<unsigned long long>(f.m.term),
              static_cast<unsigned long long>(f.m.snapshot.index),
              static_cast<unsigned long long>(f.m.offset), f.m.data.size(), f.m.done ? 1 : 0,
              f.m.success ? 1 : 0, static_cast<unsigned long long>(f.m.match_index));
        }
        r->Step(f.m);
        Collect(f.m.to);
      }
      if (!violation_.empty()) {
        return;
      }
    }
  }

  void ApplyCommitted(NodeId id) {
    auto& n = nodes_[id];
    for (Entry& e : n.raft->TakeCommittedEntries()) {
      const Index expected =
          n.applied.empty() ? n.storage->Snapshot().index + 1 : n.applied.back().index + 1;
      if (e.index != expected) {
        Fail("node " + std::to_string(id) + " applied index " + std::to_string(e.index) +
             ", expected " + std::to_string(expected));
        return;
      }
      if (!CheckCommitted(id, e)) {
        return;
      }
      n.applied.push_back(std::move(e));
    }
    if (options_.compact_every > 0 && !n.applied.empty()) {
      const Index applied = n.applied.back().index;
      const Index snap = n.storage->Snapshot().index;
      if (applied >= snap + static_cast<Index>(options_.compact_every) &&
          applied > static_cast<Index>(options_.compact_keep)) {
        (void)n.raft->Compact(applied - static_cast<Index>(options_.compact_keep));
      }
    }
  }

  bool CheckCommitted(NodeId id, const Entry& e) {
    const auto [it, inserted] = committed_.try_emplace(e.index, e.term, e.data);
    if (!inserted && (it->second.first != e.term || it->second.second != e.data)) {
      Fail("state machine safety: index " + std::to_string(e.index) + " applied as term " +
           std::to_string(it->second.first) + " elsewhere but term " + std::to_string(e.term) +
           " on node " + std::to_string(id));
      return false;
    }
    return true;
  }

  void CheckLeader(NodeId id) {
    RaftNode* r = raft(id);
    if (r->role() != Role::kLeader) {
      return;
    }
    const auto [it, inserted] = leaders_by_term_.try_emplace(r->term(), id);
    if (!inserted && it->second != id) {
      Fail("election safety: nodes " + std::to_string(it->second) + " and " + std::to_string(id) +
           " both leader in term " + std::to_string(r->term()));
    }
  }

  void Fail(const std::string& what) {
    if (violation_.empty()) {
      std::ostringstream os;
      os << "[seed " << options_.seed << ", tick " << now_ << "] " << what;
      violation_ = os.str();
    }
  }

  SimOptions options_;
  std::mt19937_64 rng_;
  std::vector<NodeId> ids_;
  std::map<NodeId, SimNode> nodes_;
  std::vector<InFlight> in_flight_;
  uint64_t now_ = 0;
  uint64_t restarts_ = 0;
  uint64_t snapshots_installed_ = 0;
  NodeId trace_node_ = kNoNode;
  uint64_t trace_from_ = 0;
  std::map<uint64_t, Index> read_floor_;
  std::vector<CompletedRead> completed_reads_;
  std::map<Term, NodeId> leaders_by_term_;
  std::map<Index, std::pair<Term, std::string>> committed_;
  std::string violation_;
};

}  // namespace kv::raft::sim

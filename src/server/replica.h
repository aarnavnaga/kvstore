#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "net/server.h"
#include "raft/file_storage.h"
#include "raft/raft.h"
#include "server/kv_state_machine.h"
#include "server/peer_transport.h"

namespace kv::server {

struct ReplicaOptions {
  raft::NodeId id = 0;
  // Every member's address (host:port), this node included. The same port serves
  // clients and peers; it is also the leader hint given to redirected clients.
  std::map<raft::NodeId, std::string> peers;
  std::string data_dir;

  int tick_ms = 10;
  int election_ticks_min = 15;  // 150-300 ms, the range the Raft paper recommends
  int election_ticks_max = 30;
  int heartbeat_ticks = 3;

  storage::Options db;
  // A client request not answered within this long gets Timeout (outcome unknown).
  std::chrono::milliseconds request_timeout{5000};
  size_t max_batch = 512;

  // Snapshot and compact the Raft log once this many entries have been applied since
  // the last snapshot, keeping snapshot_keep entries so briefly lagging followers can
  // still catch up from the log instead of needing a full snapshot.
  uint64_t snapshot_threshold = 10000;
  uint64_t snapshot_keep = 1000;

  // Reads use ReadIndex (no log entry, no fsync). read_through_log instead proposes
  // every GET as a log entry; it exists only so the benchmark can compare the two.
  bool read_through_log = false;

  // join: this node's data was wiped; rejoin without voting until caught up.
  bool join = false;

  // UNSAFE, for validating the chaos harness only: answer GETs from local state on any
  // node, skipping ReadIndex. A follower or a deposed leader then serves stale reads,
  // which the linearizability checker must catch.
  bool unsafe_local_reads = false;

  // Mutual TLS for connections to peers (the server side is configured on net::Server).
  std::shared_ptr<net::TlsContext> tls;
};

struct ReplicaStatus {
  raft::NodeId id = 0;
  raft::Role role = raft::Role::kFollower;
  raft::Term term = 0;
  raft::NodeId leader = raft::kNoNode;
  raft::Index commit = 0;
  raft::Index applied = 0;
  raft::Index last_index = 0;
  uint64_t apply_hash = 0;
  raft::Index snapshot_index = 0;
  uint64_t snapshots_installed = 0;
  uint64_t snapshots_sent = 0;
  bool joined = true;
  uint64_t ticks = 0;  // Raft clock ticks so far; the chaos harness compares clock rates
  uint64_t leader_changes = 0;
  uint64_t proposals = 0;         // client commands proposed by this node as leader
  uint64_t proposal_batches = 0;  // log appends they were grouped into
  uint64_t log_appends = 0;       // fsynced Raft log appends (leader and follower)
  double log_sync_seconds = 0;    // total time in those fsyncs
  uint64_t peer_messages_dropped = 0;

  [[nodiscard]] std::string ToString() const;
};

// Replica is one node of the cluster: Raft, its durable log, the state machine and the
// peer transport, driven by a single Raft thread.
//
// Network threads never touch Raft. They turn requests into events on an inbox; the Raft
// thread drains it, steps Raft, ticks the clock every tick_ms, proposes all queued client
// writes as one batch (one fsync for the batch), sends outgoing messages, applies
// committed entries and answers the clients waiting on them. One thread owning all Raft
// state means no locks inside the consensus logic at all.
class Replica {
 public:
  static Status Open(ReplicaOptions options, std::unique_ptr<Replica>* out);
  ~Replica();

  Replica(const Replica&) = delete;
  Replica& operator=(const Replica&) = delete;

  void Start();
  void Stop();

  // Handle is the network entry point for client and peer frames.
  void Handle(const net::Frame& frame, const net::Responder& respond);

  [[nodiscard]] ReplicaStatus status() const;
  KvStateMachine* state_machine() { return sm_.get(); }

 private:
  struct Proposal {
    Command cmd;
    net::Responder respond;
    std::chrono::steady_clock::time_point deadline;
  };
  struct Pending {
    raft::Term term;
    net::Responder respond;
    std::chrono::steady_clock::time_point deadline;
  };
  struct Read {
    std::string key;
    net::Responder respond;
    std::chrono::steady_clock::time_point deadline;
  };

  explicit Replica(ReplicaOptions options) : options_(std::move(options)) {}

  void Run();
  void ProposeBatch(std::vector<Proposal> batch);
  void ApplyCommitted();
  void StartReads(std::vector<Read> reads);
  void ServeReads();
  void MaybeCompact();
  void ExpirePending();
  std::shared_ptr<const raft::Snapshot> SnapshotSource();
  void Fail(const net::Responder& respond, const Status& s) const;
  void Wake();
  void PublishStatus();
  void RespondNotLeader(const net::Responder& respond) const;

  const ReplicaOptions options_;
  std::unique_ptr<raft::FileLogStorage> log_;
  std::unique_ptr<KvStateMachine> sm_;
  std::unique_ptr<raft::RaftNode> raft_;
  std::unique_ptr<PeerTransport> transport_;

  // The inbox is guarded by a mutex; producers signal an eventfd and the Raft thread
  // waits on it with poll() and a relative timeout until the next tick. (A condition
  // variable's absolute deadline bypasses libfaketime, which the clock-skew scenario
  // relies on to run one node's clock at 2x; see DESIGN.md bug log #11.)
  std::mutex inbox_mu_;
  int wake_fd_ = -1;
  std::deque<raft::Message> inbox_msgs_;
  std::vector<Proposal> inbox_proposals_;
  std::vector<Read> inbox_reads_;

  // Raft thread only:
  std::map<raft::Index, Pending> pending_;
  uint64_t next_read_ctx_ = 0;
  std::map<uint64_t, std::vector<Read>> unconfirmed_reads_;  // by ReadIndex ctx
  std::multimap<raft::Index, Read> confirmed_reads_;         // by read index
  std::shared_ptr<const raft::Snapshot> cached_snapshot_;
  raft::Index installed_through_ = 0;
  uint64_t ticks_ = 0;
  uint64_t leader_changes_ = 0;
  raft::NodeId last_leader_ = raft::kNoNode;
  uint64_t proposals_ = 0;
  uint64_t proposal_batches_ = 0;

  mutable std::mutex status_mu_;
  ReplicaStatus status_;

  std::atomic<bool> stopping_{false};
  std::thread thread_;
};

}  // namespace kv::server

#include "server/node_metrics.h"

#include <chrono>

#include "common/status.h"

namespace kv::server {
namespace {

constexpr const char* kCodeNames[8] = {"ok",      "not_found",  "corruption", "io_error",
                                       "invalid", "not_leader", "timeout",    "unavailable"};
constexpr const char* kOpNames[3] = {"put", "get", "delete"};

int OpIndex(net::MsgType t) {
  switch (t) {
    case net::MsgType::kPut:
      return 0;
    case net::MsgType::kGet:
      return 1;
    case net::MsgType::kDelete:
      return 2;
    default:
      return -1;
  }
}

}  // namespace

RequestMetrics::RequestMetrics(Registry* registry) {
  for (int op = 0; op < 3; ++op) {
    const std::string op_label = std::string("op=\"") + kOpNames[op] + "\"";
    for (int code = 0; code < 8; ++code) {
      ops_[op].by_code[code] =
          registry->AddCounter("kv_requests_total", "Client requests by operation and result.",
                               op_label + ",code=\"" + kCodeNames[code] + "\"");
    }
    ops_[op].latency =
        registry->AddHistogram("kv_request_duration_seconds",
                               "Time from a request being decoded to its response being queued.",
                               op_label, LatencyBuckets());
  }
}

net::Responder RequestMetrics::Instrument(const net::Frame& request, net::Responder respond) {
  const int op = OpIndex(request.type);
  if (op < 0) {
    return respond;
  }
  const auto start = std::chrono::steady_clock::now();
  return [this, op, start, respond = std::move(respond)](net::Frame response) {
    const auto code = response.payload.empty() ? 0u : static_cast<uint8_t>(response.payload[0]);
    ops_[op].by_code[code < 8 ? code : 0]->Inc();
    ops_[op].latency->Observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    respond(std::move(response));
  };
}

void RegisterNodeMetrics(Registry* r, const net::Server* server, const Replica* replica,
                         std::function<storage::DBStats()> db_stats) {
  r->AddCallback("kv_connections_open", "Open client and peer connections.", "gauge", "",
                 [server] { return static_cast<double>(server->stats().connections_open); });
  r->AddCallback("kv_connections_accepted_total", "Connections accepted.", "counter", "",
                 [server] { return static_cast<double>(server->stats().connections_accepted); });
  r->AddCallback("kv_protocol_errors_total", "Connections closed for malformed frames.", "counter",
                 "", [server] { return static_cast<double>(server->stats().protocol_errors); });
  r->AddCallback("kv_tls_handshake_failures_total",
                 "TLS handshakes refused (no certificate, or one from another CA).", "counter", "",
                 [server] { return static_cast<double>(server->stats().tls_failures); });

  auto db = std::make_shared<std::function<storage::DBStats()>>(std::move(db_stats));
  r->AddCallback("lsm_tables", "Live SSTables.", "gauge", "",
                 [db] { return static_cast<double>((*db)().num_tables); });
  r->AddCallback("lsm_table_bytes", "Bytes in live SSTables.", "gauge", "",
                 [db] { return static_cast<double>((*db)().table_bytes); });
  r->AddCallback("lsm_memtable_bytes", "Bytes in the active memtable.", "gauge", "",
                 [db] { return static_cast<double>((*db)().memtable_bytes); });
  r->AddCallback("lsm_flushes_total", "Memtable flushes.", "counter", "",
                 [db] { return static_cast<double>((*db)().flushes); });
  r->AddCallback("lsm_compactions_total", "Compactions.", "counter", "",
                 [db] { return static_cast<double>((*db)().compactions); });
  r->AddCallback("lsm_compaction_bytes_written_total", "Bytes written by compaction.", "counter",
                 "", [db] { return static_cast<double>((*db)().compaction_bytes_written); });

  if (replica == nullptr) {
    return;
  }
  const auto st = [replica] { return replica->status(); };
  r->AddCallback("raft_term", "Current Raft term.", "gauge", "",
                 [st] { return static_cast<double>(st().term); });
  r->AddCallback("raft_is_leader", "1 if this node is the leader.", "gauge", "",
                 [st] { return st().role == raft::Role::kLeader ? 1.0 : 0.0; });
  r->AddCallback("raft_has_leader", "1 if this node knows a leader.", "gauge", "",
                 [st] { return st().leader != raft::kNoNode ? 1.0 : 0.0; });
  r->AddCallback("raft_commit_index", "Highest committed log index known.", "gauge", "",
                 [st] { return static_cast<double>(st().commit); });
  r->AddCallback("raft_applied_index", "Highest log index applied to the state machine.", "gauge",
                 "", [st] { return static_cast<double>(st().applied); });
  r->AddCallback("raft_last_index", "Last index in the local log.", "gauge", "",
                 [st] { return static_cast<double>(st().last_index); });
  r->AddCallback("raft_snapshot_index", "Index the log was last compacted to.", "gauge", "",
                 [st] { return static_cast<double>(st().snapshot_index); });
  r->AddCallback("raft_leader_changes_total", "Times this node saw a new leader.", "counter", "",
                 [st] { return static_cast<double>(st().leader_changes); });
  r->AddCallback("raft_proposals_total", "Client commands proposed while leader.", "counter", "",
                 [st] { return static_cast<double>(st().proposals); });
  r->AddCallback("raft_proposal_batches_total",
                 "Log appends those proposals were batched into (one fsync each).", "counter", "",
                 [st] { return static_cast<double>(st().proposal_batches); });
  r->AddCallback("raft_log_appends_total", "Fsynced Raft log appends.", "counter", "",
                 [st] { return static_cast<double>(st().log_appends); });
  r->AddCallback("raft_log_fsync_seconds_total", "Time spent fsyncing the Raft log.", "counter", "",
                 [st] { return st().log_sync_seconds; });
  r->AddCallback("raft_snapshots_installed_total", "Snapshots received from a leader.", "counter",
                 "", [st] { return static_cast<double>(st().snapshots_installed); });
  r->AddCallback("raft_snapshots_sent_total", "Snapshots sent to followers.", "counter", "",
                 [st] { return static_cast<double>(st().snapshots_sent); });
  r->AddCallback("raft_peer_messages_dropped_total",
                 "Raft messages dropped because a peer was unreachable or slow.", "counter", "",
                 [st] { return static_cast<double>(st().peer_messages_dropped); });
}

}  // namespace kv::server

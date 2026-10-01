// kvserver: one node of the store.
//
// Cluster node:
//   kvserver --id=1 --peers=1=node1:7000,2=node2:7000,3=node3:7000 \
//            --listen=0.0.0.0:7000 --data=/var/lib/kvstore
//
// Mutual TLS (every connection, peers and clients alike, needs a cluster certificate):
//   --tls_ca=ca.pem --tls_cert=node.pem --tls_key=node-key.pem
//
// After replacing a node's disk, restart it with --join so it cannot vote with a log
// that has forgotten acknowledged entries.
//
// Operators: --admin_listen=0.0.0.0:9100 (the default) serves /metrics (Prometheus),
// /healthz (process up) and /readyz (knows a leader and is not rejoining). Empty disables.
//
// Single node, no replication (storage and network benchmarks):
//   kvserver --standalone --listen=0.0.0.0:7000 --data=/var/lib/kvstore

#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <functional>

#include "common/flags.h"
#include "net/server.h"
#include "server/admin_http.h"
#include "server/metrics.h"
#include "server/node_metrics.h"
#include "server/replica.h"
#include "server/standalone_service.h"
#include "storage/db.h"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void OnSignal(int /*sig*/) { g_stop = 1; }

bool ParsePeers(const std::string& spec, std::map<kv::raft::NodeId, std::string>* peers) {
  for (const std::string& part : kv::SplitList(spec)) {
    const size_t eq = part.find('=');
    if (eq == std::string::npos) {
      return false;
    }
    (*peers)[std::stoull(part.substr(0, eq))] = part.substr(eq + 1);
  }
  return !peers->empty();
}

using FrameHandler = std::function<void(const kv::net::Frame&, const kv::net::Responder&)>;

// Node bundles what differs between cluster and standalone mode.
struct Node {
  FrameHandler handle;
  std::function<kv::storage::DBStats()> db_stats;
  const kv::server::Replica* replica = nullptr;  // null in standalone mode
  std::function<bool()> ready;
};

int Serve(const kv::Flags& flags, const std::shared_ptr<kv::net::TlsContext>& tls,
          const Node& node) {
  kv::net::ServerOptions options;
  options.tls = tls;
  if (!kv::net::ParseHostPort(flags.Get("listen", "0.0.0.0:7000"), &options.host, &options.port)) {
    std::fprintf(stderr, "bad --listen address\n");
    return 2;
  }
  options.io_threads = static_cast<int>(flags.GetInt("io_threads", 2));
  options.worker_threads = static_cast<int>(flags.GetInt("workers", 8));

  kv::server::Registry registry;
  kv::server::RequestMetrics requests(&registry);
  kv::net::Server server(options, [&](const kv::net::Frame& f, const kv::net::Responder& r) {
    node.handle(f, requests.Instrument(f, r));
  });
  kv::Status s = server.Start();
  if (!s.ok()) {
    std::fprintf(stderr, "start server: %s\n", s.ToString().c_str());
    return 1;
  }
  kv::server::RegisterNodeMetrics(&registry, &server, node.replica, node.db_stats);
  std::fprintf(stderr, "kvserver listening on %s:%u%s\n", options.host.c_str(), server.port(),
               tls ? " (mutual TLS)" : "");

  std::unique_ptr<kv::server::AdminHttpServer> admin;
  const std::string admin_addr = flags.Get("admin_listen", "0.0.0.0:9100");
  if (!admin_addr.empty()) {
    std::string host;
    uint16_t port = 0;
    if (!kv::net::ParseHostPort(admin_addr, &host, &port)) {
      std::fprintf(stderr, "bad --admin_listen address\n");
      return 2;
    }
    using Response = kv::server::AdminHttpServer::Response;
    admin = std::make_unique<kv::server::AdminHttpServer>(
        host, port,
        std::map<std::string, kv::server::AdminHttpServer::Handler>{
            {"/metrics",
             [&registry] { return Response{200, "text/plain; version=0.0.4", registry.Render()}; }},
            {"/healthz", [] { return Response{200, "text/plain", "ok\n"}; }},
            {"/readyz",
             [&node] {
               return node.ready() ? Response{200, "text/plain", "ready\n"}
                                   : Response{503, "text/plain", "no leader yet\n"};
             }},
        });
    s = admin->Start();
    if (!s.ok()) {
      std::fprintf(stderr, "start admin server: %s\n", s.ToString().c_str());
      return 1;
    }
    std::fprintf(stderr, "admin endpoint on %s:%u\n", host.c_str(), admin->port());
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (g_stop == 0) {
    ::pause();
  }
  if (admin) {
    admin->Stop();
  }
  server.Stop();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const std::string data = flags.Get("data", "./data");
  std::shared_ptr<kv::net::TlsContext> tls;
  if (const kv::Status s = kv::net::TlsFromFiles(flags.Get("tls_ca"), flags.Get("tls_cert"),
                                                 flags.Get("tls_key"), &tls);
      !s.ok()) {
    std::fprintf(stderr, "tls: %s\n", s.ToString().c_str());
    return 2;
  }

  if (flags.GetBool("standalone", false)) {
    kv::storage::Options db_options;
    db_options.sync = flags.GetBool("sync", true);
    std::unique_ptr<kv::storage::DB> db;
    const kv::Status s = kv::storage::DB::Open(db_options, data, &db);
    if (!s.ok()) {
      std::fprintf(stderr, "open db: %s\n", s.ToString().c_str());
      return 1;
    }
    kv::server::StandaloneService service(db.get());
    Node node;
    node.handle = [&service](const kv::net::Frame& f, const kv::net::Responder& r) {
      service.Handle(f, r);
    };
    node.db_stats = [&db] { return db->GetStats(); };
    node.ready = [] { return true; };
    return Serve(flags, tls, node);
  }

  kv::server::ReplicaOptions options;
  options.id = static_cast<kv::raft::NodeId>(flags.GetInt("id", 0));
  if (options.id == 0 || !ParsePeers(flags.Get("peers"), &options.peers)) {
    std::fprintf(stderr, "cluster mode needs --id and --peers=1=host:port,2=host:port,...\n");
    return 2;
  }
  options.data_dir = data;
  options.tick_ms = static_cast<int>(flags.GetInt("tick_ms", 10));
  options.snapshot_threshold = static_cast<uint64_t>(flags.GetInt("snapshot_threshold", 10000));
  options.snapshot_keep = static_cast<uint64_t>(flags.GetInt("snapshot_keep", 1000));
  options.read_through_log = flags.Get("reads", "readindex") == "log";
  // --join: this node's disk was wiped; rejoin as a non-voter until caught up.
  options.join = flags.GetBool("join", false);
  options.unsafe_local_reads = flags.GetBool("unsafe_local_reads", false);
  options.db.sync = flags.GetBool("sync", false);  // the Raft log is the durable record
  options.tls = tls;
  std::unique_ptr<kv::server::Replica> replica;
  const kv::Status s = kv::server::Replica::Open(options, &replica);
  if (!s.ok()) {
    std::fprintf(stderr, "open replica: %s\n", s.ToString().c_str());
    return 1;
  }
  replica->Start();
  Node node;
  node.handle = [&replica](const kv::net::Frame& f, const kv::net::Responder& r) {
    replica->Handle(f, r);
  };
  node.db_stats = [&replica] { return replica->state_machine()->db()->GetStats(); };
  node.replica = replica.get();
  node.ready = [&replica] {
    const auto st = replica->status();
    return st.joined && st.leader != kv::raft::kNoNode;
  };
  const int rc = Serve(flags, tls, node);
  replica->Stop();
  return rc;
}

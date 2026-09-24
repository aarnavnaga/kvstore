// kvserver: one node of the store.
//
// Cluster node:
//   kvserver --id=1 --peers=1=node1:7000,2=node2:7000,3=node3:7000 \
//            --listen=0.0.0.0:7000 --data=/var/lib/kvstore
//
// After replacing a node's disk, restart it with --join so it cannot vote with a log
// that has forgotten acknowledged entries.
//
// Single node, no replication (storage and network benchmarks):
//   kvserver --standalone --listen=0.0.0.0:7000 --data=/var/lib/kvstore

#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <functional>

#include "common/flags.h"
#include "net/server.h"
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

int Serve(const kv::Flags& flags, const FrameHandler& handler) {
  kv::net::ServerOptions options;
  if (!kv::net::ParseHostPort(flags.Get("listen", "0.0.0.0:7000"), &options.host, &options.port)) {
    std::fprintf(stderr, "bad --listen address\n");
    return 2;
  }
  options.io_threads = static_cast<int>(flags.GetInt("io_threads", 2));
  options.worker_threads = static_cast<int>(flags.GetInt("workers", 8));
  kv::net::Server server(
      options, [&handler](const kv::net::Frame& f, const kv::net::Responder& r) { handler(f, r); });
  const kv::Status s = server.Start();
  if (!s.ok()) {
    std::fprintf(stderr, "start server: %s\n", s.ToString().c_str());
    return 1;
  }
  std::fprintf(stderr, "kvserver listening on %s:%u\n", options.host.c_str(), server.port());
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (g_stop == 0) {
    ::pause();
  }
  server.Stop();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const std::string data = flags.Get("data", "./data");

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
    return Serve(flags, [&service](const kv::net::Frame& f, const kv::net::Responder& r) {
      service.Handle(f, r);
    });
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
  std::unique_ptr<kv::server::Replica> replica;
  const kv::Status s = kv::server::Replica::Open(options, &replica);
  if (!s.ok()) {
    std::fprintf(stderr, "open replica: %s\n", s.ToString().c_str());
    return 1;
  }
  replica->Start();
  const int rc = Serve(flags, [&replica](const kv::net::Frame& f, const kv::net::Responder& r) {
    replica->Handle(f, r);
  });
  replica->Stop();
  return rc;
}

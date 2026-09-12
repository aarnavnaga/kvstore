// kvserver: one node of the store.
//
//   kvserver --listen=0.0.0.0:7000 --data=/var/lib/kvstore

#include <unistd.h>

#include <csignal>
#include <cstdio>

#include "common/flags.h"
#include "net/server.h"
#include "server/standalone_service.h"
#include "storage/db.h"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void OnSignal(int /*sig*/) { g_stop = 1; }

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  std::string host;
  uint16_t port = 0;
  if (!kv::net::ParseHostPort(flags.Get("listen", "0.0.0.0:7000"), &host, &port)) {
    std::fprintf(stderr, "bad --listen address\n");
    return 2;
  }

  kv::storage::Options db_options;
  db_options.sync = flags.GetBool("sync", true);
  std::unique_ptr<kv::storage::DB> db;
  kv::Status s = kv::storage::DB::Open(db_options, flags.Get("data", "./data"), &db);
  if (!s.ok()) {
    std::fprintf(stderr, "open db: %s\n", s.ToString().c_str());
    return 1;
  }

  kv::server::StandaloneService service(db.get());
  kv::net::ServerOptions server_options;
  server_options.host = host;
  server_options.port = port;
  server_options.io_threads = static_cast<int>(flags.GetInt("io_threads", 2));
  server_options.worker_threads = static_cast<int>(flags.GetInt("workers", 8));
  kv::net::Server server(server_options, [&service](kv::net::Frame f, kv::net::Responder r) {
    service.Handle(std::move(f), r);
  });
  s = server.Start();
  if (!s.ok()) {
    std::fprintf(stderr, "start server: %s\n", s.ToString().c_str());
    return 1;
  }
  std::fprintf(stderr, "kvserver listening on %s:%u\n", host.c_str(), server.port());

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (g_stop == 0) {
    ::pause();
  }
  server.Stop();
  return 0;
}

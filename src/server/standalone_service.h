#pragma once

#include "net/server.h"
#include "storage/db.h"

namespace kv::server {

// StandaloneService serves the client protocol straight from one local DB, with no
// replication. It is the M3 single-node server, and remains useful to benchmark the
// storage engine and network path without Raft in the way.
class StandaloneService {
 public:
  explicit StandaloneService(storage::DB* db) : db_(db) {}

  void Handle(net::Frame request, const net::Responder& respond);

 private:
  storage::DB* db_;
};

}  // namespace kv::server

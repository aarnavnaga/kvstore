#pragma once

#include <array>
#include <functional>

#include "net/server.h"
#include "server/metrics.h"
#include "server/replica.h"
#include "storage/db.h"

namespace kv::server {

// RequestMetrics counts client requests by operation and result and times them, by
// wrapping the responder: the clock stops when the response is handed to the network.
class RequestMetrics {
 public:
  explicit RequestMetrics(Registry* registry);

  // Instrument returns a responder that records the request's outcome, then forwards.
  net::Responder Instrument(const net::Frame& request, net::Responder respond);

 private:
  struct Op {
    std::array<Counter*, 8> by_code{};  // indexed by Status::Code
    Histogram* latency = nullptr;
  };
  Op ops_[3];  // put, get, delete
};

// RegisterNodeMetrics exposes server, Raft and storage state as scrape-time gauges.
// replica may be null (standalone mode); db must outlive the registry's use.
void RegisterNodeMetrics(Registry* registry, const net::Server* server, const Replica* replica,
                         std::function<storage::DBStats()> db_stats);

}  // namespace kv::server

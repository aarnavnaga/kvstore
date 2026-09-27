#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "net/client.h"
#include "net/protocol.h"

namespace kv::client {

struct ClientOptions {
  // Any subset of the cluster's client addresses; the client finds the leader itself.
  std::vector<std::string> servers;
  net::Millis request_timeout{2000};
  // Total time one operation may spend across retries before failing with Timeout.
  net::Millis operation_timeout{10000};
  net::Millis backoff_initial{20};
  net::Millis backoff_max{500};
  // 0 picks a random id. Two clients must never share one.
  uint64_t client_id = 0;
  // sticky_leader remembers the leader between operations. Off, every operation starts
  // at the configured servers again (it still follows redirects within the operation).
  bool sticky_leader = true;
};

// KvClient talks to a cluster: it follows NotLeader redirects, retries on connection
// failures with exponential backoff, and tags every write with (client_id, seq) so a
// retry of a write that actually committed is not applied a second time.
//
// A Timeout result means the outcome is unknown: the write may or may not have been
// applied. Thread-safe; operations are serialized.
class KvClient {
 public:
  explicit KvClient(ClientOptions options);

  Status Put(std::string_view key, std::string_view value);
  // Get returns NotFound if the key does not exist.
  Status Get(std::string_view key, std::string* value);
  Status Delete(std::string_view key);
  Status Ping(const std::string& addr);
  // Status returns the raw status text of one node.
  Status NodeStatus(const std::string& addr, std::string* out);

  [[nodiscard]] uint64_t client_id() const { return client_id_; }
  [[nodiscard]] std::string leader();

 private:
  Status Execute(net::KvRequest req, net::KvResponse* resp);
  Status Attempt(const std::string& addr, const net::KvRequest& req, net::KvResponse* resp);
  Status CallNode(const std::string& addr, net::MsgType type, std::string payload,
                  net::Frame* response);

  const ClientOptions options_;
  const uint64_t client_id_;
  std::mutex mu_;
  uint64_t next_seq_ = 1;
  size_t next_server_ = 0;
  std::string leader_;
  std::map<std::string, std::unique_ptr<net::Connection>> conns_;
};

}  // namespace kv::client

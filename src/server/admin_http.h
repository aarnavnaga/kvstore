#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <thread>

#include "common/status.h"

namespace kv::server {

// AdminHttpServer is a minimal HTTP/1.1 server for operators and orchestrators:
// Prometheus scrapes /metrics, Kubernetes probes /healthz and /readyz. It handles one
// request per connection on a single thread, which is plenty for a handful of scrapes
// and probes per second, and keeps this traffic off the data-path event loops.
class AdminHttpServer {
 public:
  struct Response {
    int status = 200;
    std::string content_type = "text/plain; version=0.0.4";
    std::string body;
  };
  using Handler = std::function<Response()>;

  AdminHttpServer(std::string host, uint16_t port, std::map<std::string, Handler> routes);
  ~AdminHttpServer();

  AdminHttpServer(const AdminHttpServer&) = delete;
  AdminHttpServer& operator=(const AdminHttpServer&) = delete;

  Status Start();
  void Stop();
  [[nodiscard]] uint16_t port() const { return port_; }

 private:
  void Run();
  void Serve(int fd);

  std::string host_;
  uint16_t port_;
  std::map<std::string, Handler> routes_;
  int listen_fd_ = -1;
  int wake_fd_ = -1;
  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace kv::server

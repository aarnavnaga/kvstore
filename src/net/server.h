#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "common/thread_pool.h"
#include "net/frame.h"

namespace kv::net {

// Responder sends the response to one request. It may be called from any thread, at any
// later time, exactly once. Calling it after the connection closed is a harmless no-op.
using Responder = std::function<void(Frame response)>;

// Handler processes one request frame. It runs on a worker thread and may respond
// synchronously or hand the Responder off (e.g. to complete when a Raft entry commits).
using Handler = std::function<void(Frame request, Responder respond)>;

struct ServerOptions {
  std::string host = "0.0.0.0";
  uint16_t port = 0;  // 0 picks an ephemeral port; see Server::port()
  int io_threads = 2;
  int worker_threads = 4;
  uint32_t max_frame_size = kDefaultMaxFrameSize;
  // A client that stops reading has this much response data buffered before it is
  // disconnected, so one slow consumer cannot exhaust server memory.
  size_t max_output_buffer = 64u << 20;
};

struct ServerStats {
  uint64_t connections_open = 0;
  uint64_t connections_accepted = 0;
  uint64_t frames_in = 0;
  uint64_t frames_out = 0;
  uint64_t protocol_errors = 0;
};

// Server is a non-blocking TCP server. One IO thread accepts; each of io_threads runs an
// epoll loop owning a share of the connections, reading and decoding frames and writing
// responses. Decoded requests run on a worker pool, so a slow handler never stalls the
// loop that serves other connections.
class Server {
 public:
  Server(ServerOptions options, Handler handler);
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  Status Start();
  void Stop();

  [[nodiscard]] uint16_t port() const { return port_; }
  [[nodiscard]] ServerStats stats() const;

 private:
  struct Conn;
  class Loop;

  void AcceptLoop();
  void Dispatch(const std::shared_ptr<Loop>& loop, const std::shared_ptr<Conn>& conn, Frame frame);

  ServerOptions options_;
  Handler handler_;
  int listen_fd_ = -1;
  int accept_wake_fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> running_{false};
  // Responders hold weak_ptrs to loops, so a response completing after Stop is dropped
  // instead of touching a destroyed loop.
  std::vector<std::shared_ptr<Loop>> loops_;
  std::unique_ptr<ThreadPool> workers_;
  std::thread accept_thread_;
  std::atomic<uint64_t> next_loop_{0};

  std::atomic<uint64_t> open_{0};
  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> frames_in_{0};
  std::atomic<uint64_t> frames_out_{0};
  std::atomic<uint64_t> protocol_errors_{0};
};

// ParseHostPort splits "host:port". Returns false if the port is missing or invalid.
bool ParseHostPort(std::string_view addr, std::string* host, uint16_t* port);

}  // namespace kv::net

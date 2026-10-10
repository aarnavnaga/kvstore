#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "common/status.h"
#include "net/frame.h"
#include "net/tls.h"

namespace kv::net {

using Millis = std::chrono::milliseconds;

// EnableKeepalive turns on aggressive TCP keepalives for a socket.
void EnableKeepalive(int fd, int idle_seconds);

// Connection is a blocking client connection with deadlines on every operation. It is
// not thread-safe; callers use one per thread or guard it.
class Connection {
 public:
  // Dial connects to "host:port", resolving host with DNS (so Docker and Kubernetes
  // service names work).
  // With tls set, the connection completes a mutual-TLS handshake before returning.
  static Status Dial(const std::string& addr, Millis timeout, std::unique_ptr<Connection>* out,
                     const std::shared_ptr<TlsContext>& tls = nullptr);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  Status Send(const Frame& frame, Millis timeout);
  // SendBytes writes pre-encoded frames, letting a caller batch many into one write.
  Status SendBytes(std::string_view bytes, Millis timeout);
  // Receive returns the next frame, whatever its request id.
  Status Receive(Frame* frame, Millis timeout);
  // Call sends a request with a fresh id and waits for the response carrying that id,
  // discarding stale responses to earlier requests that timed out.
  Status Call(MsgType type, std::string payload, Frame* response, Millis timeout);

  [[nodiscard]] const std::string& addr() const { return addr_; }

 private:
  Connection(int fd, std::string addr) : fd_(fd), addr_(std::move(addr)) {}
  Status Handshake(std::chrono::steady_clock::time_point deadline);

  int fd_;
  SSL* ssl_ = nullptr;
  std::string addr_;
  FrameDecoder decoder_;
  uint64_t next_id_ = 1;
};

}  // namespace kv::net

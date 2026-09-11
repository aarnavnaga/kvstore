#include "net/client.h"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

#include "net/server.h"

namespace kv::net {
namespace {

using Clock = std::chrono::steady_clock;

// WaitFd waits until fd is ready for events or the deadline passes.
Status WaitFd(int fd, short events, Clock::time_point deadline) {
  while (true) {
    const auto left = std::chrono::duration_cast<Millis>(deadline - Clock::now()).count();
    if (left <= 0) {
      return Status::Timeout("deadline exceeded");
    }
    pollfd p{fd, events, 0};
    const int n = ::poll(&p, 1, static_cast<int>(left));
    if (n > 0) {
      return Status::OK();
    }
    if (n < 0 && errno != EINTR) {
      return Status::IOErrorFromErrno("poll");
    }
  }
}

}  // namespace

Status Connection::Dial(const std::string& addr, Millis timeout, std::unique_ptr<Connection>* out) {
  std::string host;
  uint16_t port = 0;
  if (!ParseHostPort(addr, &host, &port)) {
    return Status::InvalidArgument("bad address '" + addr + "'");
  }
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  const int gai = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
  if (gai != 0) {
    return Status::Unavailable("resolve " + host + ": " + ::gai_strerror(gai));
  }
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    ::freeaddrinfo(res);
    return Status::IOErrorFromErrno("socket");
  }
  const int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);
  ::freeaddrinfo(res);
  if (rc != 0 && errno != EINPROGRESS) {
    ::close(fd);
    return Status::Unavailable("connect " + addr + ": " + ErrnoString(errno));
  }
  if (rc != 0) {
    Status s = WaitFd(fd, POLLOUT, Clock::now() + timeout);
    int err = 0;
    socklen_t len = sizeof(err);
    if (s.ok()) {
      ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    }
    if (!s.ok() || err != 0) {
      ::close(fd);
      return s.ok() ? Status::Unavailable("connect " + addr + ": " + ErrnoString(err))
                    : Status::Unavailable("connect " + addr + ": timed out");
    }
  }
  const int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  out->reset(new Connection(fd, addr));
  return Status::OK();
}

Connection::~Connection() { ::close(fd_); }

Status Connection::Send(const Frame& frame, Millis timeout) {
  const std::string bytes = EncodeFrame(frame);
  const auto deadline = Clock::now() + timeout;
  size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t n = ::send(fd_, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) {
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      KV_RETURN_IF_ERROR(WaitFd(fd_, POLLOUT, deadline));
      continue;
    }
    return Status::Unavailable("send to " + addr_ + ": " + ErrnoString(errno));
  }
  return Status::OK();
}

Status Connection::Receive(Frame* frame, Millis timeout) {
  const auto deadline = Clock::now() + timeout;
  char buf[64 * 1024];
  while (true) {
    switch (decoder_.Next(frame)) {
      case FrameDecoder::Result::kFrame:
        return Status::OK();
      case FrameDecoder::Result::kError:
        return Status::Corruption("bad frame from " + addr_ + ": " + decoder_.error());
      case FrameDecoder::Result::kNeedMore:
        break;
    }
    const ssize_t n = ::recv(fd_, buf, sizeof(buf), 0);
    if (n > 0) {
      decoder_.Feed(std::string_view(buf, static_cast<size_t>(n)));
      continue;
    }
    if (n == 0) {
      return Status::Unavailable("connection to " + addr_ + " closed");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      KV_RETURN_IF_ERROR(WaitFd(fd_, POLLIN, deadline));
      continue;
    }
    return Status::Unavailable("recv from " + addr_ + ": " + ErrnoString(errno));
  }
}

Status Connection::Call(MsgType type, std::string payload, Frame* response, Millis timeout) {
  const auto deadline = Clock::now() + timeout;
  Frame req;
  req.type = type;
  req.request_id = next_id_++;
  req.payload = std::move(payload);
  KV_RETURN_IF_ERROR(Send(req, timeout));
  while (true) {
    const auto left = std::chrono::duration_cast<Millis>(deadline - Clock::now());
    if (left.count() <= 0) {
      return Status::Timeout("no response from " + addr_);
    }
    KV_RETURN_IF_ERROR(Receive(response, left));
    if (response->request_id == req.request_id) {
      return Status::OK();
    }
  }
}

}  // namespace kv::net

#include "net/client.h"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>

#include "net/server.h"

namespace kv::net {
namespace {

using Clock = std::chrono::steady_clock;

}  // namespace

void EnableKeepalive(int fd, int idle_seconds) {
  // Probe an idle connection after idle_seconds, then every second; drop it after two
  // unanswered probes. Catches dead peers on connections with nothing to send.
  const int one = 1;
  const int interval = 1;
  const int count = 2;
  ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
  ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle_seconds, sizeof(idle_seconds));
  ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval));
  ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count));
}

namespace {

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

Status Connection::Dial(const std::string& addr, Millis timeout, std::unique_ptr<Connection>* out,
                        const std::shared_ptr<TlsContext>& tls) {
  const auto deadline = Clock::now() + timeout;
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
  // A peer whose IP vanished (a rescheduled Kubernetes pod) never resets the connection:
  // writes are buffered and retransmitted for ~15 minutes before failing. Fail the socket
  // once data has gone unacknowledged for 3 s instead, so the next Dial re-resolves DNS.
  const int user_timeout_ms = 3000;
  ::setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout_ms, sizeof(user_timeout_ms));
  EnableKeepalive(fd, 2);
  std::unique_ptr<Connection> conn(new Connection(fd, addr));
  if (tls) {
    conn->ssl_ = tls->NewClient(fd);
    KV_RETURN_IF_ERROR(conn->Handshake(deadline));
  }
  *out = std::move(conn);
  return Status::OK();
}

Status Connection::Handshake(Clock::time_point deadline) {
  while (true) {
    const int rc = SSL_do_handshake(ssl_);
    if (rc == 1) {
      return Status::OK();
    }
    const int err = SSL_get_error(ssl_, rc);
    if (err == SSL_ERROR_WANT_READ) {
      KV_RETURN_IF_ERROR(WaitFd(fd_, POLLIN, deadline));
    } else if (err == SSL_ERROR_WANT_WRITE) {
      KV_RETURN_IF_ERROR(WaitFd(fd_, POLLOUT, deadline));
    } else {
      return Status::Unavailable("TLS handshake with " + addr_ + ": " + TlsError());
    }
  }
}

Connection::~Connection() {
  SSL_free(ssl_);
  ::close(fd_);
}

Status Connection::Send(const Frame& frame, Millis timeout) {
  return SendBytes(EncodeFrame(frame), timeout);
}

Status Connection::SendBytes(std::string_view bytes, Millis timeout) {
  const auto deadline = Clock::now() + timeout;
  size_t sent = 0;
  while (sent < bytes.size()) {
    if (ssl_ != nullptr) {
      const int n = SSL_write(ssl_, bytes.data() + sent,
                              static_cast<int>(std::min<size_t>(bytes.size() - sent, 1u << 30)));
      if (n > 0) {
        sent += static_cast<size_t>(n);
        continue;
      }
      const int err = SSL_get_error(ssl_, n);
      if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
        KV_RETURN_IF_ERROR(WaitFd(fd_, err == SSL_ERROR_WANT_WRITE ? POLLOUT : POLLIN, deadline));
        continue;
      }
      return Status::Unavailable("TLS send to " + addr_ + ": " + TlsError());
    }
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
    if (ssl_ != nullptr) {
      const int n = SSL_read(ssl_, buf, sizeof(buf));
      if (n > 0) {
        decoder_.Feed(std::string_view(buf, static_cast<size_t>(n)));
        continue;
      }
      const int err = SSL_get_error(ssl_, n);
      if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        KV_RETURN_IF_ERROR(WaitFd(fd_, err == SSL_ERROR_WANT_READ ? POLLIN : POLLOUT, deadline));
        continue;
      }
      ERR_clear_error();
      return Status::Unavailable("TLS connection to " + addr_ + " closed");
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

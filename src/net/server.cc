#include "net/server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <mutex>
#include <unordered_map>

#include "net/client.h"

namespace kv::net {

bool ParseHostPort(std::string_view addr, std::string* host, uint16_t* port) {
  const size_t colon = addr.rfind(':');
  if (colon == std::string_view::npos || colon + 1 == addr.size()) {
    return false;
  }
  unsigned value = 0;
  const auto [ptr, ec] = std::from_chars(addr.data() + colon + 1, addr.data() + addr.size(), value);
  if (ec != std::errc() || ptr != addr.data() + addr.size() || value > 65535) {
    return false;
  }
  host->assign(addr.substr(0, colon));
  *port = static_cast<uint16_t>(value);
  return true;
}

// ---------------------------------------------------------------------------------------
// Connection and event loop

struct Server::Conn {
  Conn(int fd, uint32_t max_frame) : fd(fd), decoder(max_frame) {}
  ~Conn() { SSL_free(ssl); }
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;

  const int fd;
  FrameDecoder decoder;
  std::string out;
  size_t out_pos = 0;
  bool closed = false;      // loop thread only
  bool want_write = false;  // EPOLLOUT registered
  SSL* ssl = nullptr;       // set when the server runs TLS
  bool handshaking = false;
};

class Server::Loop : public std::enable_shared_from_this<Server::Loop> {
 public:
  explicit Loop(Server* server) : server_(server) {}

  ~Loop() {
    for (auto& [fd, conn] : conns_) {
      ::close(fd);
    }
    if (wake_fd_ >= 0) {
      ::close(wake_fd_);
    }
    if (epoll_fd_ >= 0) {
      ::close(epoll_fd_);
    }
  }

  Status Init() {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
      return Status::IOErrorFromErrno("epoll_create1");
    }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
      return Status::IOErrorFromErrno("eventfd");
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = wake_fd_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) != 0) {
      return Status::IOErrorFromErrno("epoll_ctl wake");
    }
    return Status::OK();
  }

  void Start() {
    thread_ = std::thread([this] { Run(); });
  }

  void Stop() {
    stopping_.store(true);
    Wake();
    if (thread_.joinable()) {
      thread_.join();
    }
    // Close connections now so clients see EOF promptly, not when the loop is destroyed.
    for (auto& [fd, conn] : conns_) {
      conn->closed = true;
      ::close(fd);
      server_->open_.fetch_sub(1);
    }
    conns_.clear();
  }

  // Post runs fn on the loop thread. Safe from any thread; dropped once the loop stops.
  void Post(std::function<void()> fn) {
    {
      std::lock_guard lk(mu_);
      tasks_.push_back(std::move(fn));
    }
    Wake();
  }

  void AddConn(int fd) {
    if (stopping_) {
      ::close(fd);
      server_->open_.fetch_sub(1);
      return;
    }
    auto conn = std::make_shared<Conn>(fd, server_->options_.max_frame_size);
    if (server_->options_.tls) {
      conn->ssl = server_->options_.tls->NewServer(fd);
      conn->handshaking = true;
    }
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
      ::close(fd);
      server_->open_.fetch_sub(1);
      return;
    }
    conns_.emplace(fd, std::move(conn));
  }

  // Send queues bytes on conn and writes as much as the socket will take right now.
  void Send(const std::shared_ptr<Conn>& conn, std::string_view bytes) {
    if (conn->closed) {
      return;
    }
    server_->frames_out_.fetch_add(1, std::memory_order_relaxed);
    conn->out.append(bytes);
    if (conn->out.size() - conn->out_pos > server_->options_.max_output_buffer) {
      Close(conn);
      return;
    }
    Flush(conn);
  }

 private:
  // IoResult is what one read or write attempt on a (plain or TLS) socket came to.
  enum class Io { kProgress, kWantRead, kWantWrite, kClosed };

  void Wake() {
    const uint64_t one = 1;
    // A full eventfd counter still means "wake up", so a failed write is fine.
    (void)!::write(wake_fd_, &one, sizeof(one));
  }

  void Run() {
    epoll_event events[256];
    while (!stopping_.load()) {
      const int n = ::epoll_wait(epoll_fd_, events, 256, 200);
      for (int i = 0; i < n; ++i) {
        const int fd = events[i].data.fd;
        if (fd == wake_fd_) {
          uint64_t v = 0;
          (void)!::read(wake_fd_, &v, sizeof(v));
          continue;
        }
        const auto it = conns_.find(fd);
        if (it == conns_.end()) {
          continue;
        }
        const std::shared_ptr<Conn> conn = it->second;
        const uint32_t e = events[i].events;
        if ((e & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) != 0) {
          Read(conn);
        }
        // TLS may need to write to make progress on a read (and vice versa), so a
        // writable socket also retries any pending output, including the handshake.
        if (!conn->closed && ((e & EPOLLOUT) != 0 || conn->out_pos < conn->out.size())) {
          Flush(conn);
        }
      }
      RunTasks();
    }
  }

  void RunTasks() {
    std::vector<std::function<void()>> tasks;
    {
      std::lock_guard lk(mu_);
      tasks.swap(tasks_);
    }
    for (auto& t : tasks) {
      t();
    }
  }

  // Handshake drives a TLS handshake forward. Returns true once it has completed.
  bool Handshake(const std::shared_ptr<Conn>& conn) {
    const int rc = SSL_do_handshake(conn->ssl);
    if (rc == 1) {
      conn->handshaking = false;
      SetWantWrite(conn, conn->out_pos < conn->out.size());
      return true;
    }
    switch (SSL_get_error(conn->ssl, rc)) {
      case SSL_ERROR_WANT_READ:
        SetWantWrite(conn, false);
        return false;
      case SSL_ERROR_WANT_WRITE:
        SetWantWrite(conn, true);
        return false;
      default:
        // No certificate, a certificate from another CA, or not TLS at all.
        server_->tls_failures_.fetch_add(1, std::memory_order_relaxed);
        ERR_clear_error();
        Close(conn);
        return false;
    }
  }

  Io ReadSome(const std::shared_ptr<Conn>& conn, char* buf, size_t len, size_t* got) {
    if (conn->ssl == nullptr) {
      while (true) {
        const ssize_t n = ::recv(conn->fd, buf, len, 0);
        if (n > 0) {
          *got = static_cast<size_t>(n);
          return Io::kProgress;
        }
        if (n < 0 && errno == EINTR) {
          continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          return Io::kWantRead;
        }
        return Io::kClosed;
      }
    }
    const int n = SSL_read(conn->ssl, buf, static_cast<int>(len));
    if (n > 0) {
      *got = static_cast<size_t>(n);
      return Io::kProgress;
    }
    switch (SSL_get_error(conn->ssl, n)) {
      case SSL_ERROR_WANT_READ:
        return Io::kWantRead;
      case SSL_ERROR_WANT_WRITE:
        return Io::kWantWrite;
      default:
        ERR_clear_error();
        return Io::kClosed;
    }
  }

  Io WriteSome(const std::shared_ptr<Conn>& conn, const char* data, size_t len, size_t* put) {
    if (conn->ssl == nullptr) {
      while (true) {
        const ssize_t n = ::send(conn->fd, data, len, MSG_NOSIGNAL);
        if (n > 0) {
          *put = static_cast<size_t>(n);
          return Io::kProgress;
        }
        if (n < 0 && errno == EINTR) {
          continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
          return Io::kWantWrite;
        }
        return Io::kClosed;
      }
    }
    const int n = SSL_write(conn->ssl, data, static_cast<int>(std::min<size_t>(len, 1u << 30)));
    if (n > 0) {
      *put = static_cast<size_t>(n);
      return Io::kProgress;
    }
    switch (SSL_get_error(conn->ssl, n)) {
      case SSL_ERROR_WANT_READ:
        return Io::kWantRead;
      case SSL_ERROR_WANT_WRITE:
        return Io::kWantWrite;
      default:
        ERR_clear_error();
        return Io::kClosed;
    }
  }

  void Read(const std::shared_ptr<Conn>& conn) {
    if (conn->handshaking && !Handshake(conn)) {
      return;
    }
    char buf[64 * 1024];
    while (true) {
      size_t got = 0;
      switch (ReadSome(conn, buf, sizeof(buf), &got)) {
        case Io::kProgress:
          conn->decoder.Feed(std::string_view(buf, got));
          continue;
        case Io::kWantRead:
          DrainFrames(conn);
          return;
        case Io::kWantWrite:
          SetWantWrite(conn, true);  // TLS needs to send before it can read more
          DrainFrames(conn);
          return;
        case Io::kClosed:
          DrainFrames(conn);
          Close(conn);
          return;
      }
    }
  }

  void DrainFrames(const std::shared_ptr<Conn>& conn) {
    Frame frame;
    while (!conn->closed) {
      switch (conn->decoder.Next(&frame)) {
        case FrameDecoder::Result::kFrame:
          server_->frames_in_.fetch_add(1, std::memory_order_relaxed);
          server_->Dispatch(shared_from_this(), conn, std::move(frame));
          frame = Frame{};
          break;
        case FrameDecoder::Result::kNeedMore:
          return;
        case FrameDecoder::Result::kError:
          server_->protocol_errors_.fetch_add(1, std::memory_order_relaxed);
          Close(conn);
          return;
      }
    }
  }

  void Flush(const std::shared_ptr<Conn>& conn) {
    if (conn->handshaking) {
      if (!Handshake(conn)) {
        return;  // output, if any, is flushed once the handshake completes
      }
      // The client's first request may already sit decrypted in OpenSSL's buffer, and
      // a level-triggered epoll will not report the socket readable for it again.
      Read(conn);
      if (conn->closed) {
        return;
      }
    }
    while (conn->out_pos < conn->out.size()) {
      size_t put = 0;
      switch (WriteSome(conn, conn->out.data() + conn->out_pos, conn->out.size() - conn->out_pos,
                        &put)) {
        case Io::kProgress:
          conn->out_pos += put;
          continue;
        case Io::kWantWrite:
          SetWantWrite(conn, true);
          return;
        case Io::kWantRead:
          return;  // TLS must read first; the next readable event retries the flush
        case Io::kClosed:
          Close(conn);
          return;
      }
    }
    conn->out.clear();
    conn->out_pos = 0;
    SetWantWrite(conn, false);
  }

  void SetWantWrite(const std::shared_ptr<Conn>& conn, bool want) {
    if (conn->want_write == want || conn->closed) {
      return;
    }
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP | (want ? EPOLLOUT : 0u);
    ev.data.fd = conn->fd;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn->fd, &ev) == 0) {
      conn->want_write = want;
    }
  }

  void Close(const std::shared_ptr<Conn>& conn) {
    if (conn->closed) {
      return;
    }
    conn->closed = true;
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, conn->fd, nullptr);
    ::close(conn->fd);
    conns_.erase(conn->fd);
    server_->open_.fetch_sub(1);
  }

  Server* server_;
  int epoll_fd_ = -1;
  int wake_fd_ = -1;
  std::thread thread_;
  std::atomic<bool> stopping_{false};
  std::mutex mu_;
  std::vector<std::function<void()>> tasks_;
  std::unordered_map<int, std::shared_ptr<Conn>> conns_;  // loop thread only
};

// ---------------------------------------------------------------------------------------
// Server

Server::Server(ServerOptions options, Handler handler)
    : options_(std::move(options)), handler_(std::move(handler)) {}

Server::~Server() {
  Stop();
  if (listen_fd_ >= 0) {  // Start failed partway
    ::close(listen_fd_);
  }
  if (accept_wake_fd_ >= 0) {
    ::close(accept_wake_fd_);
  }
}

Status Server::Start() {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  addrinfo* res = nullptr;
  const std::string port = std::to_string(options_.port);
  const int gai = ::getaddrinfo(options_.host.empty() ? nullptr : options_.host.c_str(),
                                port.c_str(), &hints, &res);
  if (gai != 0) {
    return Status::IOError("resolve " + options_.host + ": " + ::gai_strerror(gai));
  }

  listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) {
    ::freeaddrinfo(res);
    return Status::IOErrorFromErrno("socket");
  }
  const int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  const int bound = ::bind(listen_fd_, res->ai_addr, res->ai_addrlen);
  ::freeaddrinfo(res);
  if (bound != 0) {
    return Status::IOErrorFromErrno("bind " + options_.host + ":" + port);
  }
  if (::listen(listen_fd_, 4096) != 0) {
    return Status::IOErrorFromErrno("listen");
  }
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
  port_ = ntohs(addr.sin_port);

  accept_wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (accept_wake_fd_ < 0) {
    return Status::IOErrorFromErrno("eventfd");
  }
  for (int i = 0; i < std::max(1, options_.io_threads); ++i) {
    auto loop = std::make_shared<Loop>(this);
    KV_RETURN_IF_ERROR(loop->Init());
    loops_.push_back(std::move(loop));
  }
  workers_ =
      std::make_unique<ThreadPool>(static_cast<size_t>(std::max(1, options_.worker_threads)));
  running_ = true;
  for (auto& loop : loops_) {
    loop->Start();
  }
  accept_thread_ = std::thread([this] { AcceptLoop(); });
  return Status::OK();
}

void Server::Stop() {
  if (!running_.exchange(false)) {
    return;
  }
  const uint64_t one = 1;
  (void)!::write(accept_wake_fd_, &one, sizeof(one));
  accept_thread_.join();
  ::close(listen_fd_);
  ::close(accept_wake_fd_);
  listen_fd_ = -1;
  accept_wake_fd_ = -1;
  for (auto& loop : loops_) {
    loop->Stop();
  }
  // Drain the workers before dropping the loops: a running handler may still respond.
  workers_.reset();
  loops_.clear();
}

void Server::AcceptLoop() {
  pollfd fds[2] = {{listen_fd_, POLLIN, 0}, {accept_wake_fd_, POLLIN, 0}};
  while (running_.load()) {
    if (::poll(fds, 2, 500) < 0 && errno != EINTR) {
      return;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      return;
    }
    if ((fds[0].revents & POLLIN) == 0) {
      continue;
    }
    while (true) {
      const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (fd < 0) {
        // EAGAIN: backlog drained. EMFILE and friends: try again on the next wakeup
        // rather than spinning.
        break;
      }
      const int one = 1;
      // Requests and responses are small and latency-bound; never wait to coalesce.
      ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      // Reap connections from peers that vanished without closing (a deleted pod).
      EnableKeepalive(fd, 10);
      open_.fetch_add(1);
      accepted_.fetch_add(1);
      const auto& loop = loops_[next_loop_.fetch_add(1) % loops_.size()];
      loop->Post([loop = loop.get(), fd] { loop->AddConn(fd); });
    }
  }
}

void Server::Dispatch(const std::shared_ptr<Loop>& loop, const std::shared_ptr<Conn>& conn,
                      Frame frame) {
  std::weak_ptr<Conn> weak_conn = conn;
  std::weak_ptr<Loop> weak_loop = loop;
  const uint64_t id = frame.request_id;
  workers_->Submit([this, weak_conn, weak_loop, id, frame = std::move(frame)]() mutable {
    // The responder captures only weak pointers: it may outlive this Server (a Raft
    // proposal can complete after shutdown), and then it must do nothing.
    Responder respond = [weak_conn, weak_loop, id](Frame response) {
      const auto l = weak_loop.lock();
      if (!l) {
        return;
      }
      response.request_id = id;
      l->Post([l = l.get(), weak_conn, bytes = EncodeFrame(response)] {
        if (auto c = weak_conn.lock()) {
          l->Send(c, bytes);
        }
      });
    };
    handler_(std::move(frame), std::move(respond));
  });
}

ServerStats Server::stats() const {
  ServerStats s;
  s.connections_open = open_.load();
  s.connections_accepted = accepted_.load();
  s.frames_in = frames_in_.load();
  s.frames_out = frames_out_.load();
  s.protocol_errors = protocol_errors_.load();
  s.tls_failures = tls_failures_.load();
  return s;
}

}  // namespace kv::net

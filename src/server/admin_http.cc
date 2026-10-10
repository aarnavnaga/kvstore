#include "server/admin_http.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>

namespace kv::server {
namespace {

const char* Reason(int status) {
  switch (status) {
    case 200:
      return "OK";
    case 404:
      return "Not Found";
    case 405:
      return "Method Not Allowed";
    case 503:
      return "Service Unavailable";
    default:
      return "Error";
  }
}

}  // namespace

AdminHttpServer::AdminHttpServer(std::string host, uint16_t port,
                                 std::map<std::string, Handler> routes)
    : host_(std::move(host)), port_(port), routes_(std::move(routes)) {}

AdminHttpServer::~AdminHttpServer() { Stop(); }

Status AdminHttpServer::Start() {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (listen_fd_ < 0) {
    return Status::IOErrorFromErrno("socket");
  }
  const int one = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);
  if (::inet_pton(AF_INET, host_.c_str(), &addr.sin_addr) != 1) {
    return Status::InvalidArgument("admin listen address must be an IPv4 literal: " + host_);
  }
  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      ::listen(listen_fd_, 64) != 0) {
    return Status::IOErrorFromErrno("bind admin " + host_ + ":" + std::to_string(port_));
  }
  socklen_t len = sizeof(addr);
  ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
  port_ = ntohs(addr.sin_port);
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  running_ = true;
  thread_ = std::thread([this] { Run(); });
  return Status::OK();
}

void AdminHttpServer::Stop() {
  if (running_.exchange(false)) {
    const uint64_t one = 1;
    (void)!::write(wake_fd_, &one, sizeof(one));
    thread_.join();
  }
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
    listen_fd_ = -1;
  }
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
    wake_fd_ = -1;
  }
}

void AdminHttpServer::Run() {
  pollfd fds[2] = {{listen_fd_, POLLIN, 0}, {wake_fd_, POLLIN, 0}};
  while (running_) {
    if (::poll(fds, 2, 1000) <= 0) {
      continue;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      return;
    }
    const int fd = ::accept4(listen_fd_, nullptr, nullptr, SOCK_CLOEXEC);
    if (fd >= 0) {
      Serve(fd);
      ::close(fd);
    }
  }
}

void AdminHttpServer::Serve(int fd) {
  // A probe that stalls must not wedge the scrape endpoint for everyone else.
  timeval tv{1, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  std::string req;
  char buf[2048];
  while (req.find("\r\n\r\n") == std::string::npos && req.size() < size_t{16} * 1024) {
    const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    if (n <= 0) {
      return;
    }
    req.append(buf, static_cast<size_t>(n));
  }
  // Request line: METHOD SP PATH SP VERSION
  const size_t sp1 = req.find(' ');
  const size_t sp2 = sp1 == std::string::npos ? std::string::npos : req.find(' ', sp1 + 1);
  Response resp;
  if (sp2 == std::string::npos) {
    resp = {400, "text/plain", "bad request\n"};
  } else {
    const std::string method = req.substr(0, sp1);
    std::string path = req.substr(sp1 + 1, sp2 - sp1 - 1);
    path = path.substr(0, path.find('?'));
    const auto it = routes_.find(path);
    if (method != "GET") {
      resp = {405, "text/plain", "GET only\n"};
    } else if (it == routes_.end()) {
      resp = {404, "text/plain", "not found\n"};
    } else {
      resp = it->second();
    }
  }
  std::string out = "HTTP/1.1 " + std::to_string(resp.status) + " " + Reason(resp.status) +
                    "\r\nContent-Type: " + resp.content_type +
                    "\r\nContent-Length: " + std::to_string(resp.body.size()) +
                    "\r\nConnection: close\r\n\r\n" + resp.body;
  size_t sent = 0;
  while (sent < out.size()) {
    const ssize_t n = ::send(fd, out.data() + sent, out.size() - sent, MSG_NOSIGNAL);
    if (n <= 0) {
      return;
    }
    sent += static_cast<size_t>(n);
  }
}

}  // namespace kv::server

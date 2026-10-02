#include "server/metrics.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>

#include "server/admin_http.h"

namespace kv::server {
namespace {

TEST(Metrics, RendersPrometheusTextFormat) {
  Registry r;
  Counter* puts = r.AddCounter("kv_requests_total", "Requests.", "op=\"put\"");
  Counter* gets = r.AddCounter("kv_requests_total", "Requests.", "op=\"get\"");
  Histogram* h = r.AddHistogram("kv_latency_seconds", "Latency.", "op=\"put\"", {0.001, 0.01});
  r.AddCallback("raft_term", "Term.", "gauge", "", [] { return 7.0; });
  puts->Inc(3);
  gets->Inc();
  h->Observe(0.0005);
  h->Observe(0.005);
  h->Observe(5);

  const std::string out = r.Render();
  EXPECT_NE(out.find("# TYPE kv_requests_total counter\n"), std::string::npos);
  EXPECT_NE(out.find("kv_requests_total{op=\"put\"} 3\n"), std::string::npos);
  EXPECT_NE(out.find("kv_requests_total{op=\"get\"} 1\n"), std::string::npos);
  // Buckets are cumulative and end with +Inf.
  EXPECT_NE(out.find("kv_latency_seconds_bucket{op=\"put\",le=\"0.001\"} 1\n"), std::string::npos);
  EXPECT_NE(out.find("kv_latency_seconds_bucket{op=\"put\",le=\"0.01\"} 2\n"), std::string::npos);
  EXPECT_NE(out.find("kv_latency_seconds_bucket{op=\"put\",le=\"+Inf\"} 3\n"), std::string::npos);
  EXPECT_NE(out.find("kv_latency_seconds_count{op=\"put\"} 3\n"), std::string::npos);
  EXPECT_NE(out.find("raft_term 7\n"), std::string::npos);
  // HELP and TYPE appear once per family, not once per series.
  EXPECT_EQ(out.find("# HELP kv_requests_total"), out.rfind("# HELP kv_requests_total"));
}

std::string HttpGet(uint16_t port, const std::string& path) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) {
    ::close(fd);
    return "";
  }
  const std::string req = "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
  (void)!::send(fd, req.data(), req.size(), 0);
  std::string out;
  char buf[4096];
  ssize_t n = 0;
  while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) {
    out.append(buf, static_cast<size_t>(n));
  }
  ::close(fd);
  return out;
}

TEST(AdminHttp, ServesRoutesAndStatusCodes) {
  std::atomic<bool> ready{false};  // read by the server thread
  using R = AdminHttpServer::Response;
  AdminHttpServer s(
      "127.0.0.1", 0,
      {{"/healthz", [] { return R{200, "text/plain", "ok\n"}; }},
       {"/readyz",
        [&] { return ready ? R{200, "text/plain", "ready\n"} : R{503, "text/plain", "no\n"}; }}});
  ASSERT_TRUE(s.Start().ok());
  EXPECT_EQ(HttpGet(s.port(), "/healthz").rfind("HTTP/1.1 200 OK", 0), 0u);
  EXPECT_EQ(HttpGet(s.port(), "/readyz").rfind("HTTP/1.1 503", 0), 0u);
  ready = true;
  EXPECT_EQ(HttpGet(s.port(), "/readyz").rfind("HTTP/1.1 200", 0), 0u);
  EXPECT_EQ(HttpGet(s.port(), "/nope").rfind("HTTP/1.1 404", 0), 0u);
  s.Stop();
}

}  // namespace
}  // namespace kv::server

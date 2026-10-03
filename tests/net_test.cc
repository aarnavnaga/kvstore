#include <dirent.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "net/client.h"
#include "net/frame.h"
#include "net/protocol.h"
#include "net/server.h"
#include "server/standalone_service.h"
#include "storage/db.h"
#include "test_util.h"

namespace kv::net {
namespace {

using namespace std::chrono_literals;

int OpenFdCount() {
  int n = 0;
  if (DIR* d = ::opendir("/proc/self/fd")) {
    while (::readdir(d) != nullptr) {  // NOLINT(concurrency-mt-unsafe)
      ++n;
    }
    ::closedir(d);
  }
  return n;
}

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds timeout = 10s) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return pred();
}

TEST(Frame, DecodesFramesSplitAtEveryByte) {
  std::string wire;
  for (uint64_t i = 0; i < 5; ++i) {
    EncodeFrame(Frame{MsgType::kPut, i, std::string(i * 100, 'x')}, &wire);
  }
  FrameDecoder dec;
  std::vector<Frame> got;
  for (const char c : wire) {
    dec.Feed(std::string_view(&c, 1));
    Frame f;
    while (dec.Next(&f) == FrameDecoder::Result::kFrame) {
      got.push_back(f);
    }
  }
  ASSERT_EQ(got.size(), 5u);
  for (uint64_t i = 0; i < 5; ++i) {
    EXPECT_EQ(got[i].request_id, i);
    EXPECT_EQ(got[i].payload.size(), i * 100);
  }
}

TEST(Frame, RejectsOversizeAndUnknownType) {
  FrameDecoder small(1024);
  std::string big = EncodeFrame(Frame{MsgType::kPut, 1, std::string(2000, 'x')});
  small.Feed(big.substr(0, 4));  // the length alone is enough to reject it
  Frame f;
  EXPECT_EQ(small.Next(&f), FrameDecoder::Result::kError);

  FrameDecoder dec;
  std::string bad = EncodeFrame(Frame{MsgType::kPing, 1, ""});
  bad[4] = 99;
  dec.Feed(bad);
  EXPECT_EQ(dec.Next(&f), FrameDecoder::Result::kError);
}

TEST(Protocol, RoundTrips) {
  KvRequest put{MsgType::kPut, "k", "v", 42, 7};
  KvRequest got;
  ASSERT_TRUE(DecodeKvRequest(MsgType::kPut, EncodeKvRequest(put), &got));
  EXPECT_EQ(got.key, "k");
  EXPECT_EQ(got.value, "v");
  EXPECT_EQ(got.client_id, 42u);
  EXPECT_EQ(got.seq, 7u);
  EXPECT_FALSE(DecodeKvRequest(MsgType::kGet, EncodeKvRequest(put), &got));

  KvResponse resp{Status::Code::kNotLeader, "", "node2:7000"};
  KvResponse r2;
  ASSERT_TRUE(DecodeKvResponse(EncodeKvResponse(resp), &r2));
  EXPECT_TRUE(StatusFromResponse(r2).IsNotLeader());
  EXPECT_EQ(r2.leader_hint, "node2:7000");
}

// The Java client asserts exactly these bytes too (WireTest.putFrameMatchesGoldenBytes).
TEST(Protocol, GoldenBytesMatchJavaClient) {
  const std::string payload = EncodeKvRequest({MsgType::kPut, "k", "v", 42, 1});
  const std::string frame = EncodeFrame(Frame{MsgType::kPut, 7, payload});
  const unsigned char golden[] = {0x0f, 0x00, 0x00, 0x00, 0x02, 0x07, 0x00, 0x00, 0x00, 0x00,
                                  0x00, 0x00, 0x00, 0x2a, 0x01, 0x01, 0x6b, 0x01, 0x76};
  EXPECT_EQ(frame, std::string(reinterpret_cast<const char*>(golden), sizeof(golden)));
}

class ServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage::Options options;
    options.sync = false;
    ASSERT_TRUE(storage::DB::Open(options, dir_.path(), &db_).ok());
    service_ = std::make_unique<server::StandaloneService>(db_.get());
  }

  void StartServer(ServerOptions options = {}) {
    options.host = "127.0.0.1";
    server_ =
        std::make_unique<Server>(options, [this](Frame f, Responder r) { service_->Handle(f, r); });
    ASSERT_TRUE(server_->Start().ok());
    addr_ = "127.0.0.1:" + std::to_string(server_->port());
  }

  test::TempDir dir_;
  std::unique_ptr<storage::DB> db_;
  std::unique_ptr<server::StandaloneService> service_;
  std::unique_ptr<Server> server_;
  std::string addr_;
};

TEST_F(ServerTest, ClientPutGetDelete) {
  StartServer();
  client::KvClient c(client::ClientOptions{.servers = {addr_}});
  ASSERT_TRUE(c.Put("hello", "world").ok());
  std::string v;
  ASSERT_TRUE(c.Get("hello", &v).ok());
  EXPECT_EQ(v, "world");
  ASSERT_TRUE(c.Delete("hello").ok());
  EXPECT_TRUE(c.Get("hello", &v).IsNotFound());
}

TEST_F(ServerTest, PipelinedRequestsMatchByRequestId) {
  StartServer();
  std::unique_ptr<Connection> conn;
  ASSERT_TRUE(Connection::Dial(addr_, 1s, &conn).ok());
  constexpr int kN = 200;
  for (int i = 0; i < kN; ++i) {
    KvRequest req{MsgType::kPut, "k" + std::to_string(i), "v" + std::to_string(i), 0, 0};
    ASSERT_TRUE(
        conn->Send(Frame{MsgType::kPut, static_cast<uint64_t>(i), EncodeKvRequest(req)}, 1s).ok());
  }
  std::vector<bool> seen(kN, false);
  for (int i = 0; i < kN; ++i) {
    Frame f;
    ASSERT_TRUE(conn->Receive(&f, 5s).ok());
    ASSERT_LT(f.request_id, static_cast<uint64_t>(kN));
    EXPECT_FALSE(seen[f.request_id]) << "duplicate response " << f.request_id;
    seen[f.request_id] = true;
  }
}

TEST_F(ServerTest, MalformedFrameClosesOnlyThatConnection) {
  StartServer();
  std::unique_ptr<Connection> good;
  ASSERT_TRUE(Connection::Dial(addr_, 1s, &good).ok());

  // A raw socket sends a frame with an invalid message type.
  std::string wire = EncodeFrame(Frame{MsgType::kPing, 1, ""});
  wire[4] = 77;
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(server_->port());
  sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)), 0);
  ASSERT_EQ(::send(fd, wire.data(), wire.size(), 0), static_cast<ssize_t>(wire.size()));
  char b = 0;
  EXPECT_EQ(::recv(fd, &b, 1, 0), 0) << "server should close a connection that sent garbage";
  ::close(fd);
  EXPECT_GE(server_->stats().protocol_errors, 1u);

  // The well-behaved connection is unaffected.
  Frame resp;
  EXPECT_TRUE(good->Call(MsgType::kPing, "", &resp, 1s).ok());
}

TEST_F(ServerTest, SlowHandlerDoesNotBlockOtherConnections) {
  std::atomic<bool> release{false};
  server_ = std::make_unique<Server>(ServerOptions{.host = "127.0.0.1", .worker_threads = 4},
                                     [&](Frame f, Responder r) {
                                       if (f.payload == "slow") {
                                         // Hand the responder to another thread, as Raft does for
                                         // writes.
                                         std::thread([&release, r] {
                                           while (!release) {
                                             std::this_thread::sleep_for(1ms);
                                           }
                                           r(Frame{MsgType::kResponse, 0, "slow-done"});
                                         }).detach();
                                         return;
                                       }
                                       r(Frame{MsgType::kResponse, 0, "fast"});
                                     });
  ASSERT_TRUE(server_->Start().ok());
  const std::string addr = "127.0.0.1:" + std::to_string(server_->port());

  std::unique_ptr<Connection> slow;
  std::unique_ptr<Connection> fast;
  ASSERT_TRUE(Connection::Dial(addr, 1s, &slow).ok());
  ASSERT_TRUE(Connection::Dial(addr, 1s, &fast).ok());
  ASSERT_TRUE(slow->Send(Frame{MsgType::kPing, 1, "slow"}, 1s).ok());
  for (int i = 0; i < 50; ++i) {
    Frame resp;
    ASSERT_TRUE(fast->Call(MsgType::kPing, "fast", &resp, 1s).ok());
    EXPECT_EQ(resp.payload, "fast");
  }
  release = true;
  Frame resp;
  ASSERT_TRUE(slow->Receive(&resp, 2s).ok());
  EXPECT_EQ(resp.payload, "slow-done");
}

// SPEC M3 acceptance: 1,000 concurrent clients, no fd leaks.
TEST_F(ServerTest, ThousandConcurrentClientsWithoutFdLeaks) {
  const int baseline = OpenFdCount();
  StartServer(ServerOptions{.io_threads = 4, .worker_threads = 8});

  constexpr int kThreads = 20;
  constexpr int kConnsPerThread = 50;
  std::atomic<int> connected{0};
  std::atomic<bool> all_connected{false};
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::vector<std::unique_ptr<Connection>> conns(kConnsPerThread);
      for (auto& c : conns) {
        if (!Connection::Dial(addr_, 5s, &c).ok()) {
          ++failures;
          return;
        }
        ++connected;
      }
      // Hold every connection open until all 1,000 exist at once.
      while (!all_connected) {
        std::this_thread::sleep_for(1ms);
      }
      for (int i = 0; i < kConnsPerThread; ++i) {
        const std::string key = "t" + std::to_string(t) + "c" + std::to_string(i);
        Frame resp;
        KvResponse kv;
        if (!conns[i]
                 ->Call(MsgType::kPut, EncodeKvRequest({MsgType::kPut, key, key, 0, 0}), &resp, 10s)
                 .ok() ||
            !conns[i]
                 ->Call(MsgType::kGet, EncodeKvRequest({MsgType::kGet, key, "", 0, 0}), &resp, 10s)
                 .ok() ||
            !DecodeKvResponse(resp.payload, &kv) || kv.value != key) {
          ++failures;
        }
      }
    });
  }
  ASSERT_TRUE(
      Eventually([&] { return connected == kThreads * kConnsPerThread || failures > 0; }, 30s));
  ASSERT_EQ(failures.load(), 0);
  ASSERT_TRUE(Eventually([&] { return server_->stats().connections_open == 1000; }));
  all_connected = true;
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(failures.load(), 0);
  EXPECT_EQ(server_->stats().connections_accepted, 1000u);

  // Clients are gone; the server must notice and release every socket.
  ASSERT_TRUE(Eventually([&] { return server_->stats().connections_open == 0; }))
      << server_->stats().connections_open << " connections still open";
  server_->Stop();
  server_.reset();
  EXPECT_EQ(OpenFdCount(), baseline) << "file descriptors leaked";
}

}  // namespace
}  // namespace kv::net

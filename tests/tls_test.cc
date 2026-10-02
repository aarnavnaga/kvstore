// Mutual TLS: the epoll server's non-blocking handshake and I/O, the blocking client, and
// a replicated cluster with every connection authenticated.

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

#include "client/kv_client.h"
#include "cluster_util.h"
#include "net/client.h"
#include "net/protocol.h"
#include "net/server.h"
#include "server/standalone_service.h"
#include "storage/db.h"
#include "test_util.h"
#include "tls_util.h"

namespace kv {
namespace {

using namespace std::chrono_literals;

class TlsServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ca_ = test::MakeCa("kvstore test CA");
    ASSERT_TRUE(storage::DB::Open(storage::Options{.sync = false}, dir_.path(), &db_).ok());
    service_ = std::make_unique<server::StandaloneService>(db_.get());
    net::ServerOptions o;
    o.host = "127.0.0.1";
    o.tls = test::TlsFor(ca_, "server");
    server_ = std::make_unique<net::Server>(
        o, [this](const net::Frame& f, const net::Responder& r) { service_->Handle(f, r); });
    ASSERT_TRUE(server_->Start().ok());
    addr_ = "127.0.0.1:" + std::to_string(server_->port());
  }

  test::Pem ca_;
  test::TempDir dir_;
  std::unique_ptr<storage::DB> db_;
  std::unique_ptr<server::StandaloneService> service_;
  std::unique_ptr<net::Server> server_;
  std::string addr_;
};

TEST_F(TlsServerTest, MutualTlsRoundTrip) {
  client::KvClient c(client::ClientOptions{.servers = {addr_}, .tls = test::TlsFor(ca_, "client")});
  ASSERT_TRUE(c.Put("secret", "value").ok());
  std::string v;
  ASSERT_TRUE(c.Get("secret", &v).ok());
  EXPECT_EQ(v, "value");
  EXPECT_EQ(server_->stats().tls_failures, 0u);
}

TEST_F(TlsServerTest, LargeValuesAndPipeliningCrossRecordBoundaries) {
  std::unique_ptr<net::Connection> conn;
  ASSERT_TRUE(net::Connection::Dial(addr_, 2s, &conn, test::TlsFor(ca_, "client")).ok());
  // 4 MB values span hundreds of 16 KB TLS records and force partial writes.
  const std::string big(4 << 20, 'x');
  for (int i = 0; i < 8; ++i) {
    net::KvRequest req{net::MsgType::kPut, "big" + std::to_string(i), big, 0, 0};
    ASSERT_TRUE(conn->Send(net::Frame{net::MsgType::kPut, static_cast<uint64_t>(i),
                                      net::EncodeKvRequest(req)},
                           10s)
                    .ok());
  }
  for (int i = 0; i < 8; ++i) {
    net::Frame f;
    ASSERT_TRUE(conn->Receive(&f, 10s).ok());
  }
  net::Frame resp;
  ASSERT_TRUE(conn->Call(net::MsgType::kGet,
                         net::EncodeKvRequest({net::MsgType::kGet, "big7", "", 0, 0}), &resp, 10s)
                  .ok());
  net::KvResponse kv;
  ASSERT_TRUE(net::DecodeKvResponse(resp.payload, &kv));
  EXPECT_EQ(kv.value.size(), big.size());
}

TEST_F(TlsServerTest, PlaintextClientIsRejected) {
  std::unique_ptr<net::Connection> conn;
  ASSERT_TRUE(net::Connection::Dial(addr_, 1s, &conn).ok());  // TCP connects fine...
  net::Frame resp;
  EXPECT_FALSE(conn->Call(net::MsgType::kPing, "", &resp, 2s).ok());  // ...then nothing
  EXPECT_GE(server_->stats().tls_failures, 1u);
}

TEST_F(TlsServerTest, CertificateFromAnotherCaIsRejected) {
  const test::Pem rogue = test::MakeCa("someone else's CA");
  std::unique_ptr<net::Connection> conn;
  const Status s = net::Connection::Dial(addr_, 2s, &conn, test::TlsFor(rogue, "intruder"));
  if (s.ok()) {
    // TLS 1.3 lets the client finish before the server has checked its certificate; the
    // rejection then shows up on first use.
    net::Frame resp;
    EXPECT_FALSE(conn->Call(net::MsgType::kPing, "", &resp, 2s).ok());
  }
  // The client usually gives up first (it does not trust our CA either); the server
  // records its side of the failed handshake on its own thread a moment later.
  for (int i = 0; i < 200 && server_->stats().tls_failures == 0; ++i) {
    std::this_thread::sleep_for(10ms);
  }
  EXPECT_GE(server_->stats().tls_failures, 1u);
}

TEST(TlsCluster, ReplicatesWithMutualTlsBetweenNodes) {
  const test::Pem ca = test::MakeCa("cluster CA");
  test::TestCluster cluster(3, {.tls = test::TlsFor(ca, "node")});
  const raft::NodeId leader = cluster.WaitForLeader();
  ASSERT_NE(leader, raft::kNoNode);
  client::ClientOptions o;
  o.servers = cluster.addrs();
  o.tls = test::TlsFor(ca, "operator");
  client::KvClient c(o);
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(c.Put("k" + std::to_string(i), "v").ok());
  }
  cluster.Stop(leader);
  ASSERT_TRUE(c.Put("after-failover", "v").ok());
  cluster.Restart(leader);
  ASSERT_TRUE(cluster.WaitForConvergence());
}

}  // namespace
}  // namespace kv

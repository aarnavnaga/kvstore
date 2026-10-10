// kvhistory: drives a cluster with concurrent random operations and records the history
// for the linearizability checker (chaos/checker).
//
//   kvhistory --servers=a:7000,b:7000,c:7000 --clients=8 --keys=10 --duration_s=30 \
//             --out=history.jsonl [--seed=N] [--read_any]
//
// --read_any sends each GET to a random node instead of the known leader, as a client
// behind a load balancer would. A correct cluster redirects it to the leader; a cluster
// that answers reads from local state (follower or deposed leader) is caught serving a
// stale value. Without it, every read lands on the leader and a stale-read bug on the
// other nodes is invisible to the checker.
//
// Each line is one operation:
//   {"client":3,"op":"put","key":"k4","value":"c3-17","call":12345,"ret":12399,"status":"ok"}
//
// status is "ok", "notfound" (a GET of an absent key) or "unknown". An unknown write
// timed out or lost its node: it may have taken effect at any point from its call
// onwards, which the checker models by letting it return at infinity. Failed reads say
// nothing about the state and are dropped. Every written value is unique, so a read
// pins down exactly which write it observed.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client/kv_client.h"
#include "common/flags.h"

namespace {

using Clock = std::chrono::steady_clock;

int64_t NowNs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
}

std::string JsonString(const std::string& s) {
  std::string out = "\"";
  for (const char c : s) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  out.push_back('"');
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const auto servers = kv::SplitList(flags.Get("servers", "127.0.0.1:7000"));
  const int clients = static_cast<int>(flags.GetInt("clients", 8));
  const int keys = static_cast<int>(flags.GetInt("keys", 10));
  const auto duration = std::chrono::seconds(flags.GetInt("duration_s", 30));
  const uint64_t seed = static_cast<uint64_t>(flags.GetInt("seed", 1));
  const std::string out_path = flags.Get("out", "history.jsonl");
  const auto op_timeout = std::chrono::milliseconds(flags.GetInt("op_timeout_ms", 5000));
  const bool read_any = flags.GetBool("read_any", false);
  std::shared_ptr<kv::net::TlsContext> tls;
  if (const kv::Status s = kv::net::TlsFromFiles(flags.Get("tls_ca"), flags.Get("tls_cert"),
                                                 flags.Get("tls_key"), &tls);
      !s.ok()) {
    std::fprintf(stderr, "tls: %s\n", s.ToString().c_str());
    return 2;
  }

  std::ofstream out(out_path, std::ios::trunc);
  if (!out) {
    std::fprintf(stderr, "cannot open %s\n", out_path.c_str());
    return 1;
  }
  std::mutex out_mu;
  std::atomic<uint64_t> total{0};
  std::atomic<uint64_t> unknown{0};
  std::atomic<uint64_t> failed_reads{0};

  const auto start = Clock::now();
  const auto stop_at = start + duration;
  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(clients));
  for (int c = 0; c < clients; ++c) {
    threads.emplace_back([&, c] {
      kv::client::ClientOptions opts;
      opts.servers = servers;
      opts.request_timeout = std::chrono::milliseconds(2000);
      opts.operation_timeout = op_timeout;
      opts.tls = tls;
      kv::client::KvClient client(opts);
      // One client pinned to each node, for --read_any. They start at their own node;
      // a correct server redirects them from there.
      std::vector<std::unique_ptr<kv::client::KvClient>> pinned;
      for (const auto& s : servers) {
        kv::client::ClientOptions o = opts;
        o.servers = {s};
        o.sticky_leader = false;
        pinned.push_back(std::make_unique<kv::client::KvClient>(o));
      }
      std::mt19937_64 rng(seed * 1000 + static_cast<uint64_t>(c));
      uint64_t counter = 0;
      while (Clock::now() < stop_at) {
        const std::string key = "k" + std::to_string(rng() % static_cast<uint64_t>(keys));
        const uint64_t dice = rng() % 100;
        std::string op;
        std::string value;
        std::string status;
        const int64_t call = NowNs(start);
        if (dice < 50) {
          op = "get";
          kv::client::KvClient& reader = read_any ? *pinned[rng() % pinned.size()] : client;
          const kv::Status s = reader.Get(key, &value);
          if (s.ok()) {
            status = "ok";
          } else if (s.IsNotFound()) {
            status = "notfound";
            value.clear();
          } else {
            ++failed_reads;
            continue;  // a failed read constrains nothing
          }
        } else if (dice < 90) {
          op = "put";
          value = "c" + std::to_string(c) + "-" + std::to_string(++counter);
          const kv::Status s = client.Put(key, value);
          status = s.ok() ? "ok" : "unknown";
        } else {
          op = "delete";
          const kv::Status s = client.Delete(key);
          status = s.ok() ? "ok" : "unknown";
        }
        const int64_t ret = NowNs(start);
        if (status == "unknown") {
          ++unknown;
        }
        std::ostringstream line;
        line << "{\"client\":" << c << ",\"op\":\"" << op << "\",\"key\":" << JsonString(key)
             << ",\"value\":" << JsonString(value) << ",\"call\":" << call << ",\"ret\":" << ret
             << ",\"status\":\"" << status << "\"}\n";
        {
          std::lock_guard lk(out_mu);
          out << line.str();
        }
        ++total;
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  out.flush();
  std::printf("kvhistory: %llu ops recorded, %llu with unknown outcome, %llu failed reads\n",
              static_cast<unsigned long long>(total.load()),
              static_cast<unsigned long long>(unknown.load()),
              static_cast<unsigned long long>(failed_reads.load()));
  return 0;
}

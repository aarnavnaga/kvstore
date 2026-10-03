// kvbench: load generator for a kvstore cluster.
//
//   kvbench --mode=load   --servers=... --keys=1000000 --value_size=1024
//       Insert keys 0..keys-1 (closed loop, deep pipelines) so reads hit real data.
//
//   kvbench --mode=closed --servers=... --workload=A --connections=8 --depth=32
//       Saturation throughput: every connection keeps `depth` requests in flight.
//
//   kvbench --mode=open   --servers=... --workload=A --rate=5000
//       Latency at a fixed offered load. Requests are scheduled at fixed intervals and
//       latency is measured from each request's *scheduled* time, not from when it was
//       actually sent, so a stall shows up in the tail instead of quietly delaying the
//       requests that would have measured it (coordinated omission).
//
//   kvbench --mode=probe  --servers=... --duration_s=8
//       Writes every 2 ms through the retrying client and reports the longest gap
//       between successful writes: the unavailability window around a leader failure.
//
//   kvbench --mode=wait   --servers=host:port
//       Polls one node until it serves a read; prints how long that took.
//
// Workloads (YCSB, Zipfian keys, theta 0.99):  A 50% read / 50% update   B 95% / 5%
//                                              C 100% read               W 100% write
// Every mode prints one JSON object on stdout.

#include <hdr/hdr_histogram.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "client/kv_client.h"
#include "common/flags.h"
#include "net/client.h"
#include "net/protocol.h"
#include "zipf.h"

namespace {

using Clock = std::chrono::steady_clock;
using kv::net::Frame;
using kv::net::MsgType;

int64_t Ns(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

std::string Key(uint64_t i) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "user%012llu", static_cast<unsigned long long>(i));
  return buf;
}

struct Workload {
  int read_percent;
};

Workload ParseWorkload(const std::string& w) {
  if (w == "A") return {50};
  if (w == "B") return {95};
  if (w == "C") return {100};
  if (w == "W") return {0};
  std::fprintf(stderr, "unknown workload %s (A, B, C or W)\n", w.c_str());
  std::exit(2);
}

// Leader finds the current leader by writing through the retrying client.
std::string Leader(const std::vector<std::string>& servers) {
  kv::client::KvClient c(kv::client::ClientOptions{.servers = servers});
  const kv::Status s = c.Put("__kvbench_probe", "x");
  if (!s.ok()) {
    std::fprintf(stderr, "cannot reach the cluster: %s\n", s.ToString().c_str());
    std::exit(1);
  }
  return c.leader();
}

// Stats merges per-thread HDR histograms (microseconds, 3 significant digits).
struct Stats {
  Stats() { hdr_init(1, 60'000'000, 3, &h); }
  ~Stats() { hdr_close(h); }
  Stats(const Stats&) = delete;
  Stats& operator=(const Stats&) = delete;
  hdr_histogram* h = nullptr;
  std::atomic<uint64_t> completed{0};
  std::atomic<uint64_t> errors{0};
};

// Pipeline drives one connection: a sender thread issues requests (on a schedule, or
// whenever fewer than `depth` are in flight), and a receiver thread matches responses to
// requests by id and records latency.
class Pipeline {
 public:
  struct Options {
    std::string addr;
    Workload workload;
    const kv::bench::ScrambledZipfian* zipf;
    uint64_t keys;
    size_t value_size;
    // Open loop: send request k at start + (k * stride + offset) / rate.
    double rate = 0;
    int stride = 1;
    int offset = 0;
    // Closed loop: keep this many requests in flight. Ignored when rate > 0.
    int depth = 1;
    // Load mode: write keys first_key, first_key + stride, ... instead of a workload.
    bool load = false;
    uint64_t first_key = 0;
    Clock::time_point start;
    Clock::time_point record_from;  // warmup ends
    Clock::time_point stop;
    uint64_t seed = 1;
  };

  Pipeline(Options o, Stats* stats) : o_(std::move(o)), stats_(stats) {}

  void Run() {
    std::unique_ptr<kv::net::Connection> conn;
    if (!kv::net::Connection::Dial(o_.addr, std::chrono::seconds(2), &conn).ok()) {
      std::fprintf(stderr, "cannot connect to %s\n", o_.addr.c_str());
      std::exit(1);
    }
    hdr_init(1, 60'000'000, 3, &local_);
    std::thread receiver([&] { Receive(conn.get()); });
    Send(conn.get());
    receiver.join();
    hdr_add(stats_->h, local_);
    hdr_close(local_);
  }

 private:
  void Send(kv::net::Connection* conn) {
    std::mt19937_64 rng(o_.seed);
    const std::string value(o_.value_size, 'v');
    uint64_t next_load_key = o_.first_key;
    for (uint64_t k = 0;; ++k) {
      Clock::time_point scheduled;
      if (o_.rate > 0) {
        scheduled =
            o_.start + std::chrono::nanoseconds(static_cast<int64_t>(
                           (static_cast<double>(k) * o_.stride + o_.offset) * 1e9 / o_.rate));
        if (scheduled >= o_.stop) {
          break;
        }
        std::this_thread::sleep_until(scheduled);
      } else {
        std::unique_lock lk(mu_);
        cv_.wait(lk, [&] { return in_flight_ < o_.depth; });
        if (Clock::now() >= o_.stop) {
          break;
        }
        scheduled = Clock::now();
      }

      Frame f;
      if (o_.load) {
        if (next_load_key >= o_.keys) {
          break;
        }
        f.type = MsgType::kPut;
        f.payload = kv::net::EncodeKvRequest({MsgType::kPut, Key(next_load_key), value, 0, 0});
        next_load_key += static_cast<uint64_t>(o_.stride);
      } else {
        const std::string key = Key(o_.zipf->Next(rng));
        if (static_cast<int>(rng() % 100) < o_.workload.read_percent) {
          f.type = MsgType::kGet;
          f.payload = kv::net::EncodeKvRequest({MsgType::kGet, key, "", 0, 0});
        } else {
          f.type = MsgType::kPut;
          // No session: the benchmark never retries, so there is nothing to deduplicate.
          f.payload = kv::net::EncodeKvRequest({MsgType::kPut, key, value, 0, 0});
        }
      }
      f.request_id = k + 1;
      {
        std::lock_guard lk(mu_);
        started_[f.request_id] = scheduled;
        ++in_flight_;
        ++sent_;
      }
      if (!conn->Send(f, std::chrono::seconds(10)).ok()) {
        std::fprintf(stderr, "send to %s failed\n", o_.addr.c_str());
        std::exit(1);
      }
    }
    std::lock_guard lk(mu_);
    done_sending_ = true;
  }

  void Receive(kv::net::Connection* conn) {
    while (true) {
      {
        std::lock_guard lk(mu_);
        if (done_sending_ && in_flight_ == 0) {
          return;
        }
      }
      Frame f;
      const kv::Status s = conn->Receive(&f, std::chrono::milliseconds(200));
      if (s.code() == kv::Status::Code::kTimeout) {
        if (Clock::now() > o_.stop + std::chrono::seconds(30)) {
          return;  // give up on responses that will never come
        }
        continue;
      }
      if (!s.ok()) {
        std::fprintf(stderr, "receive from %s failed: %s\n", o_.addr.c_str(), s.ToString().c_str());
        std::exit(1);
      }
      const auto now = Clock::now();
      Clock::time_point began;
      {
        std::lock_guard lk(mu_);
        const auto it = started_.find(f.request_id);
        if (it == started_.end()) {
          continue;
        }
        began = it->second;
        started_.erase(it);
        --in_flight_;
      }
      cv_.notify_one();
      kv::net::KvResponse r;
      const bool ok = kv::net::DecodeKvResponse(f.payload, &r) &&
                      (r.code == kv::Status::Code::kOk || r.code == kv::Status::Code::kNotFound);
      if (began < o_.record_from) {
        continue;  // warmup
      }
      if (!ok) {
        stats_->errors.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      stats_->completed.fetch_add(1, std::memory_order_relaxed);
      hdr_record_value(local_, std::max<int64_t>(1, (Ns(now) - Ns(began)) / 1000));
    }
  }

  Options o_;
  Stats* stats_;
  hdr_histogram* local_ = nullptr;
  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<uint64_t, Clock::time_point> started_;
  int in_flight_ = 0;
  uint64_t sent_ = 0;
  bool done_sending_ = false;
};

void PrintJson(const kv::Flags& flags, const std::string& mode, const Stats& st, double seconds) {
  const auto pct = [&](double p) {
    return static_cast<long long>(hdr_value_at_percentile(st.h, p));
  };
  std::printf(
      "{\"label\":\"%s\",\"mode\":\"%s\",\"workload\":\"%s\",\"nodes\":%lld,\"target_rate\":%lld,"
      "\"seconds\":%.2f,\"completed\":%llu,\"errors\":%llu,\"throughput\":%.1f,"
      "\"p50_us\":%lld,\"p90_us\":%lld,\"p99_us\":%lld,\"p999_us\":%lld,\"max_us\":%lld,"
      "\"mean_us\":%.1f}\n",
      flags.Get("label", "").c_str(), mode.c_str(), flags.Get("workload", "").c_str(),
      static_cast<long long>(flags.GetInt("nodes", 0)),
      static_cast<long long>(flags.GetInt("rate", 0)), seconds,
      static_cast<unsigned long long>(st.completed.load()),
      static_cast<unsigned long long>(st.errors.load()),
      static_cast<double>(st.completed.load()) / seconds, pct(50), pct(90), pct(99), pct(99.9),
      static_cast<long long>(hdr_max(st.h)), hdr_mean(st.h));
}

int RunLoad(const kv::Flags& flags, const std::vector<std::string>& servers) {
  const std::string leader = Leader(servers);
  const int conns = static_cast<int>(flags.GetInt("connections", 8));
  Stats st;
  const auto start = Clock::now();
  std::vector<std::thread> threads;
  for (int c = 0; c < conns; ++c) {
    Pipeline::Options o;
    o.addr = leader;
    o.keys = static_cast<uint64_t>(flags.GetInt("keys", 1'000'000));
    o.value_size = static_cast<size_t>(flags.GetInt("value_size", 1024));
    o.depth = static_cast<int>(flags.GetInt("depth", 64));
    o.load = true;
    o.first_key = static_cast<uint64_t>(c);
    o.stride = conns;
    o.start = start;
    o.record_from = start;
    o.stop = start + std::chrono::hours(24);
    threads.emplace_back([o, &st] { Pipeline(o, &st).Run(); });
  }
  for (auto& t : threads) {
    t.join();
  }
  PrintJson(flags, "load", st, std::chrono::duration<double>(Clock::now() - start).count());
  return 0;
}

int RunWorkload(const kv::Flags& flags, const std::vector<std::string>& servers, bool open) {
  const std::string leader = Leader(servers);
  const Workload w = ParseWorkload(flags.Get("workload", "A"));
  const auto keys = static_cast<uint64_t>(flags.GetInt("keys", 1'000'000));
  const kv::bench::ScrambledZipfian zipf(keys);
  const int conns = static_cast<int>(flags.GetInt("connections", 8));
  const double rate = static_cast<double>(flags.GetInt("rate", 1000));
  const auto warmup = std::chrono::seconds(flags.GetInt("warmup_s", 3));
  const auto duration = std::chrono::seconds(flags.GetInt("duration_s", 20));

  Stats st;
  const auto start = Clock::now() + std::chrono::milliseconds(100);
  std::vector<std::thread> threads;
  for (int c = 0; c < conns; ++c) {
    Pipeline::Options o;
    o.addr = leader;
    o.workload = w;
    o.zipf = &zipf;
    o.keys = keys;
    o.value_size = static_cast<size_t>(flags.GetInt("value_size", 1024));
    o.rate = open ? rate : 0;
    o.stride = conns;
    o.offset = c;
    o.depth = static_cast<int>(flags.GetInt("depth", 32));
    o.start = start;
    o.record_from = start + warmup;
    o.stop = start + warmup + duration;
    o.seed = static_cast<uint64_t>(flags.GetInt("seed", 1)) * 1000 + static_cast<uint64_t>(c);
    threads.emplace_back([o, &st] { Pipeline(o, &st).Run(); });
  }
  for (auto& t : threads) {
    t.join();
  }
  PrintJson(flags, open ? "open" : "closed", st, std::chrono::duration<double>(duration).count());
  return 0;
}

int RunProbe(const kv::Flags& flags, const std::vector<std::string>& servers) {
  kv::client::ClientOptions o;
  o.servers = servers;
  o.request_timeout = std::chrono::milliseconds(150);
  o.operation_timeout = std::chrono::seconds(30);
  o.backoff_initial = std::chrono::milliseconds(2);
  o.backoff_max = std::chrono::milliseconds(20);
  kv::client::KvClient c(o);
  const auto stop = Clock::now() + std::chrono::seconds(flags.GetInt("duration_s", 8));
  Clock::time_point last_ok = Clock::now();
  double max_gap_ms = 0;
  uint64_t writes = 0;
  for (uint64_t i = 0; Clock::now() < stop; ++i) {
    if (c.Put("__kvbench_failover", std::to_string(i)).ok()) {
      const auto now = Clock::now();
      max_gap_ms =
          std::max(max_gap_ms, std::chrono::duration<double, std::milli>(now - last_ok).count());
      last_ok = now;
      ++writes;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  std::printf("{\"mode\":\"probe\",\"writes\":%llu,\"max_gap_ms\":%.1f}\n",
              static_cast<unsigned long long>(writes), max_gap_ms);
  return 0;
}

int RunWait(const kv::Flags& flags, const std::vector<std::string>& servers) {
  const auto start = Clock::now();
  const auto deadline = start + std::chrono::seconds(flags.GetInt("timeout_s", 300));
  while (Clock::now() < deadline) {
    std::unique_ptr<kv::net::Connection> conn;
    if (kv::net::Connection::Dial(servers[0], std::chrono::milliseconds(200), &conn).ok()) {
      Frame resp;
      kv::net::KvResponse r;
      if (conn->Call(MsgType::kGet, kv::net::EncodeKvRequest({MsgType::kGet, Key(0), "", 0, 0}),
                     &resp, std::chrono::milliseconds(500))
              .ok() &&
          kv::net::DecodeKvResponse(resp.payload, &r) &&
          (r.code == kv::Status::Code::kOk || r.code == kv::Status::Code::kNotFound)) {
        std::printf("{\"mode\":\"wait\",\"ready_ms\":%.1f}\n",
                    std::chrono::duration<double, std::milli>(Clock::now() - start).count());
        return 0;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::printf("{\"mode\":\"wait\",\"ready_ms\":-1}\n");
  return 1;
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const auto servers = kv::SplitList(flags.Get("servers", "127.0.0.1:7000"));
  const std::string mode = flags.Get("mode", "closed");
  if (mode == "load") return RunLoad(flags, servers);
  if (mode == "closed") return RunWorkload(flags, servers, false);
  if (mode == "open") return RunWorkload(flags, servers, true);
  if (mode == "probe") return RunProbe(flags, servers);
  if (mode == "wait") return RunWait(flags, servers);
  std::fprintf(stderr, "unknown --mode %s\n", mode.c_str());
  return 2;
}

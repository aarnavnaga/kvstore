// storage_bench: the LSM engine alone, in-process: no Raft, no network. Measures what
// replication costs on top (compare with kvbench) and what group commit buys.
//
//   storage_bench --dir=/tmp/b --threads=16 --seconds=10 --value_size=1024
//
// Prints one JSON object per measurement:
//   writes, sync, fsync per write    every Put waits for its own fsync
//   writes, sync, group commit       concurrent Puts share fsyncs
//   writes, no sync                  WAL written but not fsynced
//   reads                            random Gets over the keys written above

#include <hdr/hdr_histogram.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/file.h"
#include "common/flags.h"
#include "storage/db.h"

namespace {

using Clock = std::chrono::steady_clock;

std::string Key(uint64_t i) {
  char buf[24];
  std::snprintf(buf, sizeof(buf), "user%012llu", static_cast<unsigned long long>(i));
  return buf;
}

struct Result {
  uint64_t ops = 0;
  double seconds = 0;
  uint64_t syncs = 0;
  hdr_histogram* h = nullptr;
};

// Run drives `threads` workers calling op(thread, i) for `seconds`, recording latency.
template <typename Op>
Result Run(int threads, double seconds, Op op) {
  Result r;
  hdr_init(1, 60'000'000, 3, &r.h);
  std::mutex mu;
  std::atomic<uint64_t> ops{0};
  const auto stop = Clock::now() + std::chrono::duration<double>(seconds);
  const auto start = Clock::now();
  std::vector<std::thread> ts;
  for (int t = 0; t < threads; ++t) {
    ts.emplace_back([&, t] {
      hdr_histogram* local = nullptr;
      hdr_init(1, 60'000'000, 3, &local);
      uint64_t n = 0;
      for (uint64_t i = 0; Clock::now() < stop; ++i) {
        const auto t0 = Clock::now();
        op(t, i);
        hdr_record_value(
            local,
            std::max<int64_t>(
                1,
                std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count()));
        ++n;
      }
      ops += n;
      std::lock_guard lk(mu);
      hdr_add(r.h, local);
      hdr_close(local);
    });
  }
  for (auto& th : ts) {
    th.join();
  }
  r.ops = ops.load();
  r.seconds = std::chrono::duration<double>(Clock::now() - start).count();
  return r;
}

void Print(const char* name, const Result& r) {
  std::printf(
      "{\"bench\":\"%s\",\"ops\":%llu,\"seconds\":%.2f,\"ops_per_sec\":%.0f,\"fsyncs\":%llu,"
      "\"p50_us\":%lld,\"p99_us\":%lld,\"p999_us\":%lld}\n",
      name, static_cast<unsigned long long>(r.ops), r.seconds,
      static_cast<double>(r.ops) / r.seconds, static_cast<unsigned long long>(r.syncs),
      static_cast<long long>(hdr_value_at_percentile(r.h, 50)),
      static_cast<long long>(hdr_value_at_percentile(r.h, 99)),
      static_cast<long long>(hdr_value_at_percentile(r.h, 99.9)));
  std::fflush(stdout);
  hdr_close(r.h);
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const std::string base = flags.Get("dir", "/tmp/storage_bench");
  const int threads = static_cast<int>(flags.GetInt("threads", 16));
  const double seconds = static_cast<double>(flags.GetInt("seconds", 10));
  const std::string value(static_cast<size_t>(flags.GetInt("value_size", 1024)), 'v');

  struct Mode {
    const char* name;
    bool sync;
    bool group;
  };
  const Mode modes[] = {{"write_fsync_per_write", true, false},
                        {"write_group_commit", true, true},
                        {"write_no_sync", false, true}};
  uint64_t written = 0;
  for (const Mode& m : modes) {
    const std::string dir = kv::JoinPath(base, m.name);
    (void)kv::RemoveDirRecursive(dir);
    kv::storage::Options o;
    o.sync = m.sync;
    o.group_commit = m.group;
    std::unique_ptr<kv::storage::DB> db;
    if (!kv::storage::DB::Open(o, dir, &db).ok()) {
      std::fprintf(stderr, "open %s failed\n", dir.c_str());
      return 1;
    }
    std::atomic<uint64_t> next{0};
    Result r =
        Run(threads, seconds, [&](int, uint64_t) { (void)db->Put(Key(next.fetch_add(1)), value); });
    r.syncs = db->GetStats().wal_syncs;
    written = std::max(written, next.load());
    Print(m.name, r);
  }

  // Reads over the largest data set written above.
  const std::string dir = kv::JoinPath(base, "write_no_sync");
  std::unique_ptr<kv::storage::DB> db;
  if (!kv::storage::DB::Open(kv::storage::Options{}, dir, &db).ok()) {
    return 1;
  }
  (void)db->CompactAll();
  std::vector<std::mt19937_64> rngs;
  for (int t = 0; t < threads; ++t) {
    rngs.emplace_back(static_cast<uint64_t>(t) + 1);
  }
  std::string v;
  Result r = Run(threads, seconds, [&](int t, uint64_t) {
    std::string out;
    (void)db->Get(Key(rngs[static_cast<size_t>(t)]() % written), &out);
  });
  Print("read_random", r);
  (void)kv::RemoveDirRecursive(base);
  return 0;
}

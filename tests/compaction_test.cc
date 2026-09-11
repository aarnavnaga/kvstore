#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/file.h"
#include "storage/db.h"
#include "test_util.h"

namespace kv::storage {
namespace {

using std::chrono::steady_clock;

std::string Key(int i) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "key%07d", i);
  return buf;
}

// WaitFor polls pred until it holds or the deadline passes.
template <typename Pred>
bool WaitFor(Pred pred, std::chrono::seconds timeout = std::chrono::seconds(60)) {
  const auto deadline = steady_clock::now() + timeout;
  while (steady_clock::now() < deadline) {
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return pred();
}

TEST(Compaction, DiskUsageDropsAfterMassDeletes) {
  test::TempDir dir;
  Options options;
  options.write_buffer_size = 256 << 10;
  options.sync = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());

  constexpr int kKeys = 40000;
  const std::string value(200, 'v');
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(db->Put(Key(i), value).ok());
  }
  // Settle to a single table so the baseline is the live data, not tiering debris.
  ASSERT_TRUE(db->CompactAll().ok());
  const uint64_t full = DirSize(dir.path());
  ASSERT_GT(full, 5u << 20);

  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(db->Delete(Key(i)).ok());
  }
  ASSERT_TRUE(db->Flush().ok());

  // No manual compaction: the tombstone-ratio trigger has to notice on its own.
  ASSERT_TRUE(WaitFor([&] { return DirSize(dir.path()) < full / 20; }))
      << "disk usage " << DirSize(dir.path()) << " bytes, was " << full;
  const DBStats stats = db->GetStats();
  EXPECT_GT(stats.compactions, 0u);
  std::printf("mass delete: %llu -> %llu bytes on disk, %llu tables\n",
              static_cast<unsigned long long>(full),
              static_cast<unsigned long long>(DirSize(dir.path())),
              static_cast<unsigned long long>(stats.num_tables));

  db.reset();
  ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());
  std::string got;
  for (int i = 0; i < kKeys; i += 97) {
    ASSERT_TRUE(db->Get(Key(i), &got).IsNotFound()) << Key(i);
  }
}

TEST(Compaction, SizeTieringBoundsTableCount) {
  test::TempDir dir;
  Options options;
  options.write_buffer_size = 32 << 10;
  options.sync = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());

  std::mt19937 rng(7);
  const std::string value(100, 'x');
  uint64_t max_tables = 0;
  for (int i = 0; i < 60000; ++i) {
    ASSERT_TRUE(db->Put(Key(static_cast<int>(rng() % 30000)), value).ok());
    if (i % 1000 == 0) {
      max_tables = std::max(max_tables, db->GetStats().num_tables);
    }
  }
  ASSERT_TRUE(db->Flush().ok());
  const DBStats stats = db->GetStats();
  EXPECT_GT(stats.flushes, 100u);
  EXPECT_GT(stats.compactions, 10u);
  // Without compaction this would be one table per flush (hundreds); tiering keeps it
  // logarithmic in the data size.
  EXPECT_LT(max_tables, 40u);
  std::printf("tiering: %llu flushes, %llu compactions, peak %llu tables\n",
              static_cast<unsigned long long>(stats.flushes),
              static_cast<unsigned long long>(stats.compactions),
              static_cast<unsigned long long>(max_tables));
}

TEST(Compaction, ReadsStayCorrectWhileCompactionRuns) {
  const uint64_t seed = test::TestSeed();
  SCOPED_TRACE("KV_SEED=" + std::to_string(seed));
  test::TempDir dir;
  Options options;
  options.write_buffer_size = 64 << 10;
  options.sync = false;
  options.compaction_min_tables = 2;  // compact as eagerly as possible
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());

  // Every key always exists and holds "<key>@<generation>". The writer bumps
  // generations; readers must never see a key missing or its generation go backwards.
  constexpr int kKeys = 2000;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(db->Put(Key(i), Key(i) + "@0").ok());
  }

  std::atomic<bool> stop{false};
  std::atomic<int> failures{0};
  std::thread writer([&] {
    std::mt19937 rng(static_cast<uint32_t>(seed));
    std::vector<int> gen(kKeys, 0);
    for (int n = 0; n < 150000 && failures == 0; ++n) {
      const int k = static_cast<int>(rng() % kKeys);
      if (!db->Put(Key(k), Key(k) + "@" + std::to_string(++gen[k])).ok()) {
        ++failures;
      }
    }
    stop = true;
  });
  std::thread compactor([&] {
    while (!stop) {
      if (!db->CompactAll().ok()) {
        ++failures;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  });
  std::vector<std::thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&, r] {
      std::mt19937 rng(static_cast<uint32_t>(seed) + r);
      std::vector<long> seen(kKeys, 0);
      while (!stop) {
        const int k = static_cast<int>(rng() % kKeys);
        std::string v;
        if (!db->Get(Key(k), &v).ok()) {
          ADD_FAILURE() << Key(k) << " missing during compaction";
          ++failures;
          return;
        }
        const long g = std::stol(v.substr(v.find('@') + 1));
        if (v.substr(0, v.find('@')) != Key(k) || g < seen[k]) {
          ADD_FAILURE() << Key(k) << " went from generation " << seen[k] << " to " << v;
          ++failures;
          return;
        }
        seen[k] = g;
      }
    });
  }
  writer.join();
  compactor.join();
  for (auto& t : readers) {
    t.join();
  }
  EXPECT_EQ(failures.load(), 0);
  EXPECT_GT(db->GetStats().compactions, 5u);

  // A scan sees each key exactly once.
  auto it = db->NewIterator();
  int n = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_EQ(it->key(), Key(n));
    ++n;
  }
  EXPECT_EQ(n, kKeys);
}

TEST(Compaction, CompactAllDropsShadowedVersionsAndTombstones) {
  test::TempDir dir;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(
      DB::Open(Options{.sync = false, .background_compaction = false}, dir.path(), &db).ok());
  for (int gen = 0; gen < 5; ++gen) {
    for (int i = 0; i < 1000; ++i) {
      ASSERT_TRUE(db->Put(Key(i), "gen" + std::to_string(gen)).ok());
    }
    ASSERT_TRUE(db->Flush().ok());
  }
  for (int i = 0; i < 1000; i += 2) {
    ASSERT_TRUE(db->Delete(Key(i)).ok());
  }
  ASSERT_TRUE(db->CompactAll().ok());
  const DBStats stats = db->GetStats();
  EXPECT_EQ(stats.num_tables, 1u);

  // One table holding exactly the 500 live keys: no stale generations, no tombstones.
  auto it = db->NewIterator();
  int live = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    EXPECT_EQ(it->value(), "gen4");
    ++live;
  }
  EXPECT_EQ(live, 500);
}

}  // namespace
}  // namespace kv::storage

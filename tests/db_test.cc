#include "storage/db.h"

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <thread>
#include <vector>

#include "test_util.h"

namespace kv::storage {
namespace {

class DBTest : public ::testing::Test {
 protected:
  void Open(Options options = {}) {
    db_.reset();
    ASSERT_TRUE(DB::Open(options, dir_.path(), &db_).ok());
  }

  std::string Get(std::string_view key) {
    std::string value;
    Status s = db_->Get(key, &value);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    EXPECT_TRUE(s.ok()) << s.ToString();
    return value;
  }

  test::TempDir dir_;
  std::unique_ptr<DB> db_;
};

TEST_F(DBTest, PutGetDelete) {
  Open();
  ASSERT_TRUE(db_->Put("a", "1").ok());
  ASSERT_TRUE(db_->Put("b", "2").ok());
  EXPECT_EQ(Get("a"), "1");
  ASSERT_TRUE(db_->Put("a", "3").ok());
  EXPECT_EQ(Get("a"), "3");
  ASSERT_TRUE(db_->Delete("a").ok());
  EXPECT_EQ(Get("a"), "NOT_FOUND");
  EXPECT_EQ(Get("b"), "2");
  EXPECT_EQ(Get("never"), "NOT_FOUND");
}

TEST_F(DBTest, RecoversFromWalAfterReopen) {
  Open();
  ASSERT_TRUE(db_->Put("k1", "v1").ok());
  ASSERT_TRUE(db_->Put("k2", "v2").ok());
  ASSERT_TRUE(db_->Delete("k1").ok());
  Open();
  EXPECT_EQ(Get("k1"), "NOT_FOUND");
  EXPECT_EQ(Get("k2"), "v2");
}

TEST_F(DBTest, SecondOpenOfSameDirectoryFails) {
  Open();
  std::unique_ptr<DB> other;
  EXPECT_FALSE(DB::Open(Options{}, dir_.path(), &other).ok());
}

TEST_F(DBTest, FlushedTablesShadowCorrectly) {
  Open(Options{.write_buffer_size = 64 << 10});
  // Three generations of the same keys across separate SSTables.
  for (int gen = 0; gen < 3; ++gen) {
    for (int i = 0; i < 500; ++i) {
      ASSERT_TRUE(db_->Put("key" + std::to_string(i), "gen" + std::to_string(gen)).ok());
    }
    ASSERT_TRUE(db_->Flush().ok());
  }
  for (int i = 0; i < 500; i += 2) {
    ASSERT_TRUE(db_->Delete("key" + std::to_string(i)).ok());
  }
  ASSERT_TRUE(db_->Flush().ok());
  EXPECT_GE(db_->GetStats().num_tables, 4u);

  for (int round = 0; round < 2; ++round) {
    for (int i = 0; i < 500; ++i) {
      EXPECT_EQ(Get("key" + std::to_string(i)), i % 2 == 0 ? "NOT_FOUND" : "gen2");
    }
    Open(Options{.write_buffer_size = 64 << 10});  // same answers after recovery
  }
}

TEST_F(DBTest, IteratorMergesSourcesAndHidesTombstones) {
  Open();
  ASSERT_TRUE(db_->Put("a", "old").ok());
  ASSERT_TRUE(db_->Put("c", "c").ok());
  ASSERT_TRUE(db_->Flush().ok());
  ASSERT_TRUE(db_->Put("a", "new").ok());
  ASSERT_TRUE(db_->Put("b", "b").ok());
  ASSERT_TRUE(db_->Delete("c").ok());

  auto it = db_->NewIterator();
  std::string seen;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    seen += std::string(it->key()) + "=" + std::string(it->value()) + ";";
  }
  EXPECT_EQ(seen, "a=new;b=b;");
}

TEST_F(DBTest, ConcurrentWritersShareFsyncs) {
  Open();
  constexpr int kThreads = 8;
  constexpr int kPerThread = 200;
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kPerThread; ++i) {
        ASSERT_TRUE(db_->Put("t" + std::to_string(t) + "-" + std::to_string(i), "v").ok());
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  const DBStats stats = db_->GetStats();
  EXPECT_EQ(stats.writes, static_cast<uint64_t>(kThreads * kPerThread));
  // With eight writers contending, group commit must have merged at least some fsyncs.
  EXPECT_LT(stats.wal_syncs, stats.writes);
  for (int t = 0; t < kThreads; ++t) {
    EXPECT_EQ(Get("t" + std::to_string(t) + "-" + std::to_string(kPerThread - 1)), "v");
  }
}

TEST_F(DBTest, ReadersRunConcurrentlyWithWritesAndFlushes) {
  Open(Options{.write_buffer_size = 32 << 10, .sync = false});
  std::atomic<bool> stop{false};
  std::atomic<int> written{0};
  std::thread writer([&] {
    for (int i = 0; i < 20000; ++i) {
      ASSERT_TRUE(db_->Put("k" + std::to_string(i), std::to_string(i)).ok());
      written.store(i + 1, std::memory_order_release);
    }
    stop = true;
  });
  std::vector<std::thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&, r] {
      std::mt19937 rng(r);
      while (!stop) {
        const int n = written.load(std::memory_order_acquire);
        if (n == 0) {
          continue;
        }
        const int i = static_cast<int>(rng() % n);
        std::string v;
        ASSERT_TRUE(db_->Get("k" + std::to_string(i), &v).ok()) << "lost key " << i;
        ASSERT_EQ(v, std::to_string(i));
      }
    });
  }
  writer.join();
  for (auto& th : readers) {
    th.join();
  }
  EXPECT_GT(db_->GetStats().flushes, 0u);
}

}  // namespace
}  // namespace kv::storage

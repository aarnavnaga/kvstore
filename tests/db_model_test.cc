// Model-based test: drive the store and a std::map with the same random operations and
// fail on the first divergence. Small memtables force constant flushes, and periodic
// reopens exercise recovery in the middle of the sequence.

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>

#include "storage/db.h"
#include "test_util.h"

namespace kv::storage {
namespace {

TEST(DBModel, MatchesStdMap) {
  const uint64_t seed = test::TestSeed();
  const int64_t ops = test::EnvInt("KV_MODEL_OPS", 1'000'000);
  SCOPED_TRACE("KV_SEED=" + std::to_string(seed));
  std::printf("DBModel seed=%llu ops=%lld\n", static_cast<unsigned long long>(seed),
              static_cast<long long>(ops));

  test::TempDir dir;
  Options options;
  options.write_buffer_size = 256 << 10;
  options.sync = false;  // durability is the crash test's job; this one checks logic
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());

  std::mt19937_64 rng(seed);
  std::map<std::string, std::string> model;
  auto random_key = [&] { return "key" + std::to_string(rng() % 20000); };
  auto random_value = [&] {
    std::string v(rng() % 120, '\0');
    for (auto& c : v) {
      c = static_cast<char>('a' + rng() % 26);
    }
    return v;
  };

  for (int64_t i = 0; i < ops; ++i) {
    const uint64_t dice = rng() % 100;
    if (dice < 45) {
      const std::string k = random_key();
      const std::string v = random_value();
      ASSERT_TRUE(db->Put(k, v).ok());
      model[k] = v;
    } else if (dice < 60) {
      const std::string k = random_key();
      ASSERT_TRUE(db->Delete(k).ok());
      model.erase(k);
    } else if (dice < 62) {
      // Multi-key atomic batch.
      WriteBatch batch;
      for (int j = 0; j < 5; ++j) {
        const std::string k = random_key();
        if (rng() % 3 == 0) {
          batch.Delete(k);
          model.erase(k);
        } else {
          const std::string v = random_value();
          batch.Put(k, v);
          model[k] = v;
        }
      }
      ASSERT_TRUE(db->Write(&batch).ok());
    } else {
      const std::string k = random_key();
      std::string got;
      const Status s = db->Get(k, &got);
      const auto it = model.find(k);
      if (it == model.end()) {
        ASSERT_TRUE(s.IsNotFound()) << "op " << i << " key " << k << " should be absent";
      } else {
        ASSERT_TRUE(s.ok()) << "op " << i << " key " << k << ": " << s.ToString();
        ASSERT_EQ(got, it->second) << "op " << i << " key " << k;
      }
    }
    if (i > 0 && i % 200'000 == 0) {
      db.reset();
      ASSERT_TRUE(DB::Open(options, dir.path(), &db).ok());
    }
  }

  // Final full scan must equal the model exactly, in order.
  auto it = db->NewIterator();
  auto want = model.begin();
  for (it->SeekToFirst(); it->Valid(); it->Next(), ++want) {
    ASSERT_NE(want, model.end()) << "extra key " << it->key();
    ASSERT_EQ(it->key(), want->first);
    ASSERT_EQ(it->value(), want->second);
  }
  ASSERT_TRUE(it->status().ok());
  EXPECT_EQ(want, model.end()) << "missing key " << want->first;
  std::printf("DBModel: %zu live keys, %llu tables, %llu flushes\n", model.size(),
              static_cast<unsigned long long>(db->GetStats().num_tables),
              static_cast<unsigned long long>(db->GetStats().flushes));
}

}  // namespace
}  // namespace kv::storage

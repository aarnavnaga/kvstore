// Unit tests for the storage engine's on-disk and in-memory building blocks.

#include <gtest/gtest.h>

#include <map>
#include <random>
#include <string>

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/file.h"
#include "storage/bloom.h"
#include "storage/manifest.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"
#include "test_util.h"

namespace kv::storage {
namespace {

TEST(Coding, VarintRoundTrip) {
  const uint64_t values[] = {0, 1, 127, 128, 300, 1u << 21, UINT32_MAX, UINT64_MAX};
  std::string buf;
  for (const uint64_t v : values) {
    PutVarint64(&buf, v);
  }
  std::string_view in(buf);
  for (const uint64_t v : values) {
    uint64_t got = 0;
    ASSERT_TRUE(GetVarint64(&in, &got));
    EXPECT_EQ(got, v);
  }
  EXPECT_TRUE(in.empty());
}

TEST(Coding, TruncatedVarintFails) {
  std::string buf;
  PutVarint64(&buf, 1u << 30);
  std::string_view in(buf.data(), buf.size() - 1);
  uint64_t v = 0;
  EXPECT_FALSE(GetVarint64(&in, &v));
}

TEST(Crc32c, KnownVectors) {
  // Standard check value for CRC-32C.
  EXPECT_EQ(crc32c::Value("123456789"), 0xE3069283u);
  EXPECT_EQ(crc32c::Value(""), 0u);
  // Extending in pieces matches one pass, across the 8-byte fast path boundary.
  const std::string s = "the quick brown fox jumps over the lazy dog";
  const uint32_t whole = crc32c::Value(s);
  for (size_t split = 0; split <= s.size(); ++split) {
    const uint32_t a = crc32c::Value(s.data(), split);
    EXPECT_EQ(crc32c::Extend(a, s.data() + split, s.size() - split), whole);
  }
}

TEST(WriteBatch, IterateReplaysInOrder) {
  WriteBatch b;
  b.Put("a", "1");
  b.Delete("b");
  b.Put("c", "");
  b.SetSequence(42);
  EXPECT_EQ(b.Count(), 3u);
  EXPECT_EQ(b.Sequence(), 42u);

  struct Rec : WriteBatch::Handler {
    std::string log;
    void Put(std::string_view k, std::string_view v) override {
      log += "P" + std::string(k) + "=" + std::string(v) + ";";
    }
    void Delete(std::string_view k) override { log += "D" + std::string(k) + ";"; }
  } rec;
  ASSERT_TRUE(b.Iterate(&rec).ok());
  EXPECT_EQ(rec.log, "Pa=1;Db;Pc=;");
}

TEST(WriteBatch, CountMismatchIsCorruption) {
  WriteBatch b;
  b.Put("a", "1");
  std::string bad(b.Contents());
  EncodeFixed32(bad.data() + 8, 2);
  WriteBatch c;
  ASSERT_TRUE(c.SetContents(bad).ok());
  struct Nop : WriteBatch::Handler {
    void Put(std::string_view, std::string_view) override {}
    void Delete(std::string_view) override {}
  } nop;
  EXPECT_TRUE(c.Iterate(&nop).IsCorruption());
}

TEST(Wal, RoundTripAndTornTail) {
  std::string log;
  for (int i = 0; i < 100; ++i) {
    log += EncodeRecord("record-" + std::to_string(i));
  }
  const size_t full = log.size();
  // Simulate a crash that persisted only part of the last record.
  log += EncodeRecord("torn-record").substr(0, 9);

  WalReader reader(log);
  std::string_view rec;
  int n = 0;
  while (reader.Next(&rec)) {
    EXPECT_EQ(rec, "record-" + std::to_string(n));
    ++n;
  }
  EXPECT_EQ(n, 100);
  EXPECT_EQ(reader.tail_dropped(), log.size() - full);
}

TEST(Wal, CorruptRecordStopsReplay) {
  std::string log = EncodeRecord("good") + EncodeRecord("flipped") + EncodeRecord("after");
  log[EncodeRecord("good").size() + 10] ^= 0x01;  // a bit flip inside the second payload
  WalReader reader(log);
  std::string_view rec;
  ASSERT_TRUE(reader.Next(&rec));
  EXPECT_EQ(rec, "good");
  EXPECT_FALSE(reader.Next(&rec));
}

TEST(Bloom, NoFalseNegativesAndLowFalsePositiveRate) {
  BloomBuilder builder(10);
  for (int i = 0; i < 10000; ++i) {
    builder.AddKey("key" + std::to_string(i));
  }
  const std::string filter = builder.Finish();
  for (int i = 0; i < 10000; ++i) {
    ASSERT_TRUE(BloomMayContain(filter, "key" + std::to_string(i)));
  }
  int false_positives = 0;
  constexpr int kProbes = 100000;
  for (int i = 0; i < kProbes; ++i) {
    false_positives += BloomMayContain(filter, "absent" + std::to_string(i)) ? 1 : 0;
  }
  const double rate = static_cast<double>(false_positives) / kProbes;
  EXPECT_LT(rate, 0.02) << "false-positive rate " << rate;
}

TEST(MemTable, NewestVersionWins) {
  MemTable mem;
  mem.Add(1, ValueType::kValue, "k", "v1");
  mem.Add(2, ValueType::kValue, "k", "v2");
  std::string value;
  ValueType type{};
  ASSERT_TRUE(mem.Get("k", &value, &type));
  EXPECT_EQ(type, ValueType::kValue);
  EXPECT_EQ(value, "v2");

  mem.Add(3, ValueType::kDeletion, "k", "");
  ASSERT_TRUE(mem.Get("k", &value, &type));
  EXPECT_EQ(type, ValueType::kDeletion);
  EXPECT_FALSE(mem.Get("other", &value, &type));
}

TEST(MemTable, IteratorYieldsOneEntryPerKeyInOrder) {
  MemTable mem;
  mem.Add(1, ValueType::kValue, "b", "b1");
  mem.Add(2, ValueType::kValue, "a", "a1");
  mem.Add(3, ValueType::kValue, "b", "b2");
  mem.Add(4, ValueType::kDeletion, "c", "");
  auto it = mem.NewIterator();
  std::string seen;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    seen += std::string(it->key()) + "=" +
            (it->type() == ValueType::kValue ? std::string(it->value()) : "X") + ";";
  }
  EXPECT_EQ(seen, "a=a1;b=b2;c=X;");
  it->Seek("b");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key(), "b");
  EXPECT_EQ(it->value(), "b2");
}

class SSTableTest : public ::testing::Test {
 protected:
  test::TempDir dir_;
};

TEST_F(SSTableTest, RoundTripGetAndIterate) {
  std::map<std::string, std::string> want;
  const std::string path = TableFileName(dir_.path(), 7);
  {
    std::unique_ptr<AppendFile> file;
    ASSERT_TRUE(AppendFile::Open(path, &file).ok());
    TableBuilder builder(TableOptions{.block_size = 256}, file.get());
    for (int i = 0; i < 2000; ++i) {
      char key[16];
      std::snprintf(key, sizeof(key), "key%06d", i);
      if (i % 10 == 0) {
        builder.Add(key, ValueType::kDeletion, "");
      } else {
        want[key] = "value-" + std::to_string(i);
        builder.Add(key, ValueType::kValue, want[key]);
      }
    }
    ASSERT_TRUE(builder.Finish().ok());
    ASSERT_TRUE(file->Close().ok());
  }

  std::shared_ptr<Table> table;
  ASSERT_TRUE(Table::Open(path, 7, &table).ok());
  EXPECT_EQ(table->num_entries(), 2000u);
  EXPECT_EQ(table->num_tombstones(), 200u);
  EXPECT_EQ(table->smallest(), "key000000");
  EXPECT_EQ(table->largest(), "key001999");

  std::string value;
  ValueType type{};
  ASSERT_TRUE(table->Get("key000005", &value, &type).ok());
  EXPECT_EQ(value, "value-5");
  ASSERT_TRUE(table->Get("key000010", &value, &type).ok());
  EXPECT_EQ(type, ValueType::kDeletion);
  EXPECT_TRUE(table->Get("key0000055", &value, &type).IsNotFound());
  EXPECT_TRUE(table->Get("zzz", &value, &type).IsNotFound());

  auto it = table->NewIterator();
  size_t live = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->type() == ValueType::kValue) {
      ASSERT_EQ(want.at(std::string(it->key())), it->value());
      ++live;
    }
  }
  EXPECT_TRUE(it->status().ok());
  EXPECT_EQ(live, want.size());

  it->Seek("key001500");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key(), "key001500");
}

TEST_F(SSTableTest, BitFlipIsDetected) {
  const std::string path = TableFileName(dir_.path(), 1);
  {
    std::unique_ptr<AppendFile> file;
    ASSERT_TRUE(AppendFile::Open(path, &file).ok());
    TableBuilder builder(TableOptions{.block_size = 64}, file.get());
    for (int i = 0; i < 100; ++i) {
      builder.Add("key" + std::to_string(1000 + i), ValueType::kValue,
                  "value" + std::to_string(1000 + i));
    }
    ASSERT_TRUE(builder.Finish().ok());
    ASSERT_TRUE(file->Close().ok());
  }
  std::string contents;
  ASSERT_TRUE(ReadFileToString(path, &contents).ok());

  // A flip in the first block is caught at open, which reads it for the smallest key.
  std::string first = contents;
  first[first.find("value1000")] ^= 0x20;
  std::shared_ptr<Table> table;
  EXPECT_TRUE(Table::OpenFromContents(first, 1, &table).IsCorruption());

  // A flip in a later block is caught by the read that touches it.
  contents[contents.find("value1090")] ^= 0x20;
  ASSERT_TRUE(Table::OpenFromContents(contents, 1, &table).ok());
  std::string value;
  ValueType type{};
  EXPECT_TRUE(table->Get("key1090", &value, &type).IsCorruption());
  EXPECT_TRUE(table->Get("key1001", &value, &type).ok());
}

TEST(Manifest, RoundTripAndChecksum) {
  ManifestState st;
  st.next_file_number = 99;
  st.log_number = 12;
  st.tables = {40, 31, 7};
  const std::string enc = EncodeManifest(st);
  ManifestState got;
  ASSERT_TRUE(DecodeManifest(enc, &got).ok());
  EXPECT_EQ(got.next_file_number, 99u);
  EXPECT_EQ(got.log_number, 12u);
  EXPECT_EQ(got.tables, st.tables);

  std::string bad = enc;
  bad[10] ^= 1;
  EXPECT_TRUE(DecodeManifest(bad, &got).IsCorruption());
}

}  // namespace
}  // namespace kv::storage

#pragma once

// SSTable: an immutable, sorted file written once by a flush or compaction.
//
//   [data block 0][crc] ... [data block n][crc] [bloom][crc] [index][crc] [footer]
//
// Data block entries:  u8 type | varint key_len | varint value_len | key | value
// Index entries:       varint key_len | last key of block | varint offset | varint size
// Footer (56 bytes):   index offset/size, bloom offset/size, entry count,
//                      tombstone count, magic   (all fixed64)
//
// Every block carries a CRC32C that is checked on read, so a flipped bit on disk surfaces
// as a Corruption status rather than a wrong value.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/file.h"
#include "common/status.h"
#include "storage/bloom.h"
#include "storage/iterator.h"

namespace kv::storage {

struct TableOptions {
  size_t block_size = 4096;
  int bloom_bits_per_key = 10;
};

class TableBuilder {
 public:
  TableBuilder(const TableOptions& options, AppendFile* file);

  // Add appends an entry. Keys must be strictly increasing.
  void Add(std::string_view key, ValueType type, std::string_view value);
  // Finish writes the bloom filter, index and footer. The caller syncs the file.
  Status Finish();

  [[nodiscard]] uint64_t NumEntries() const { return num_entries_; }
  [[nodiscard]] uint64_t FileSize() const { return offset_; }

 private:
  void FlushBlock();
  void WriteBlock(std::string_view contents);

  TableOptions options_;
  AppendFile* file_;
  BloomBuilder bloom_;
  std::string block_;
  std::string index_;
  std::string last_key_;
  uint64_t offset_ = 0;
  uint64_t num_entries_ = 0;
  uint64_t num_tombstones_ = 0;
  Status status_;
};

struct BlockEntry {
  ValueType type = ValueType::kValue;
  std::string_view key;
  std::string_view value;
};

// DecodeBlockEntry parses one data-block entry from the front of *in. It never reads past
// in's bounds and returns false on malformed input (exposed for the fuzzer).
bool DecodeBlockEntry(std::string_view* in, BlockEntry* entry);

class Table : public std::enable_shared_from_this<Table> {
 public:
  static Status Open(const std::string& path, uint64_t number, std::shared_ptr<Table>* out);

  // OpenFromContents parses an in-memory table image. Used by Open and the fuzzer.
  static Status OpenFromContents(std::string_view contents, uint64_t number,
                                 std::shared_ptr<Table>* out);

  // Get finds key, setting *type and (for values) *value. Returns NotFound if the table
  // has no entry for key.
  Status Get(std::string_view key, std::string* value, ValueType* type) const;

  // NewIterator returns an iterator that keeps this table alive while it exists.
  [[nodiscard]] std::unique_ptr<Iterator> NewIterator() const;

  [[nodiscard]] uint64_t number() const { return number_; }
  [[nodiscard]] uint64_t file_size() const { return contents_.size(); }
  [[nodiscard]] uint64_t num_entries() const { return num_entries_; }
  [[nodiscard]] uint64_t num_tombstones() const { return num_tombstones_; }
  [[nodiscard]] const std::string& smallest() const { return smallest_; }
  [[nodiscard]] const std::string& largest() const { return largest_; }

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

 private:
  class Iter;
  struct IndexEntry {
    std::string last_key;
    uint64_t offset;
    uint64_t size;
  };

  Table() = default;
  Status Parse();
  // ReadBlock returns block i's contents after checking its CRC.
  Status ReadBlock(size_t i, std::string_view* block) const;

  std::unique_ptr<MappedFile> file_;
  std::string_view contents_;
  uint64_t number_ = 0;
  std::vector<IndexEntry> index_;
  std::string_view bloom_;
  uint64_t num_entries_ = 0;
  uint64_t num_tombstones_ = 0;
  std::string smallest_;
  std::string largest_;
};

// TableFileName returns "<dir>/<number>.sst" with the number zero-padded to six digits.
std::string TableFileName(const std::string& dir, uint64_t number);

}  // namespace kv::storage

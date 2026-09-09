#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "storage/arena.h"
#include "storage/iterator.h"
#include "storage/skiplist.h"

namespace kv::storage {

// MemTable is the in-memory write buffer: a skiplist of entries ordered by key ascending,
// then by sequence number descending, so the newest version of a key is found first.
// Add requires external synchronization; Get and iterators are safe concurrently with Add.
class MemTable {
 public:
  MemTable();
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  void Add(uint64_t seq, ValueType type, std::string_view key, std::string_view value);

  // Get looks up the newest entry for key. Returns false if the memtable has no entry;
  // otherwise sets *type and, for a value, *value.
  bool Get(std::string_view key, std::string* value, ValueType* type) const;

  // NewIterator yields only the newest entry per key, tombstones included.
  [[nodiscard]] std::unique_ptr<Iterator> NewIterator() const;

  [[nodiscard]] size_t ApproximateMemoryUsage() const { return arena_.MemoryUsage(); }
  [[nodiscard]] uint64_t NumEntries() const { return num_entries_.load(std::memory_order_relaxed); }
  [[nodiscard]] bool Empty() const { return NumEntries() == 0; }

 private:
  class Iter;

  // KeyComparator orders encoded entries: user key ascending, then sequence descending.
  struct KeyComparator {
    int operator()(const char* a, const char* b) const;
  };
  using Table = SkipList<const char*, KeyComparator>;

  Arena arena_;
  Table table_;
  std::atomic<uint64_t> num_entries_{0};
};

}  // namespace kv::storage

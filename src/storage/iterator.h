#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace kv::storage {

// ValueType tags every stored entry. A deletion is a tombstone: it shadows older values
// of the same key until compaction can prove nothing older remains and drops both.
enum class ValueType : unsigned char {
  kDeletion = 0,
  kValue = 1,
};

// Iterator walks entries in ascending key order. Internal iterators (memtable, SSTable,
// merge) surface tombstones; the DB iterator hides them. key() and value() stay valid
// only until the iterator moves.
class Iterator {
 public:
  virtual ~Iterator() = default;

  [[nodiscard]] virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  // Seek positions at the first entry with key >= target.
  virtual void Seek(std::string_view target) = 0;
  virtual void Next() = 0;
  [[nodiscard]] virtual std::string_view key() const = 0;
  [[nodiscard]] virtual std::string_view value() const = 0;
  [[nodiscard]] virtual ValueType type() const = 0;
  [[nodiscard]] virtual Status status() const { return Status::OK(); }
};

// NewMergingIterator merges children ordered newest-first. When several children hold
// the same key, the entry from the lowest-index (newest) child wins and the rest are
// skipped, so each key appears exactly once.
std::unique_ptr<Iterator> NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children);

// NewLiveIterator wraps an internal iterator and hides tombstones.
std::unique_ptr<Iterator> NewLiveIterator(std::unique_ptr<Iterator> internal);

}  // namespace kv::storage

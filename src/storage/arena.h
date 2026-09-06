#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

namespace kv::storage {

// Arena is a bump allocator for memtable entries. Everything it hands out is freed at
// once when the memtable is dropped, so there is no per-entry free and no fragmentation.
// Allocation is single-threaded (the memtable has one writer at a time); MemoryUsage may
// be read concurrently.
class Arena {
 public:
  Arena() = default;
  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  char* Allocate(size_t bytes);
  // AllocateAligned returns memory aligned for pointers, for skiplist nodes whose
  // next-pointer array is accessed atomically.
  char* AllocateAligned(size_t bytes);

  [[nodiscard]] size_t MemoryUsage() const { return usage_.load(std::memory_order_relaxed); }

 private:
  static constexpr size_t kBlockSize = 4096;

  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t bytes);

  char* ptr_ = nullptr;
  size_t remaining_ = 0;
  std::vector<std::unique_ptr<char[]>> blocks_;
  std::atomic<size_t> usage_{0};
};

inline char* Arena::Allocate(size_t bytes) {
  if (bytes <= remaining_) {
    char* result = ptr_;
    ptr_ += bytes;
    remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace kv::storage

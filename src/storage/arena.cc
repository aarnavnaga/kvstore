#include "storage/arena.h"

#include <cstdint>

namespace kv::storage {

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Large values get their own block so the rest of the current block isn't wasted.
    return AllocateNewBlock(bytes);
  }
  ptr_ = AllocateNewBlock(kBlockSize);
  remaining_ = kBlockSize;
  char* result = ptr_;
  ptr_ += bytes;
  remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  constexpr size_t kAlign = alignof(std::max_align_t);
  const size_t mod = reinterpret_cast<uintptr_t>(ptr_) & (kAlign - 1);
  const size_t slop = mod == 0 ? 0 : kAlign - mod;
  if (bytes + slop <= remaining_) {
    char* result = ptr_ + slop;
    ptr_ += bytes + slop;
    remaining_ -= bytes + slop;
    return result;
  }
  // New blocks come from operator new[], which is max_align_t aligned.
  return AllocateFallback(bytes);
}

char* Arena::AllocateNewBlock(size_t bytes) {
  blocks_.push_back(std::make_unique<char[]>(bytes));
  usage_.fetch_add(bytes + sizeof(char*), std::memory_order_relaxed);
  return blocks_.back().get();
}

}  // namespace kv::storage

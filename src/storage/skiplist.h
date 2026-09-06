#pragma once

// A concurrent skiplist: one writer at a time (callers serialize inserts), any number of
// readers with no locking. Nodes are never removed, so a reader can never see freed
// memory, and each node is fully initialized before a release-store links it in, so a
// reader that acquire-loads a pointer sees the node's contents.

#include <atomic>
#include <cassert>
#include <cstdint>

#include "storage/arena.h"

namespace kv::storage {

template <typename Key, typename Comparator>
class SkipList {
 private:
  struct Node;

 public:
  SkipList(Comparator cmp, Arena* arena)
      : cmp_(cmp), arena_(arena), head_(NewNode(Key{}, kMaxHeight)) {
    for (int i = 0; i < kMaxHeight; ++i) {
      head_->SetNext(i, nullptr);
    }
  }

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert adds key. Requires external synchronization with other Inserts, and that no
  // equal key is already present.
  void Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);
    assert(x == nullptr || cmp_(key, x->key) != 0);
    (void)x;

    const int height = RandomHeight();
    const int max_height = max_height_.load(std::memory_order_relaxed);
    if (height > max_height) {
      for (int i = max_height; i < height; ++i) {
        prev[i] = head_;
      }
      // Readers that see the new height before the new node is linked find nullptr at
      // the new levels and drop down a level, which is still correct.
      max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; ++i) {
      x->NoBarrierSetNext(i, prev[i]->NoBarrierNext(i));
      prev[i]->SetNext(i, x);
    }
  }

  class Iterator {
   public:
    explicit Iterator(const SkipList* list) : list_(list) {}

    [[nodiscard]] bool Valid() const { return node_ != nullptr; }
    [[nodiscard]] const Key& key() const { return node_->key; }
    void Next() { node_ = node_->Next(0); }
    void Seek(const Key& target) { node_ = list_->FindGreaterOrEqual(target, nullptr); }
    void SeekToFirst() { node_ = list_->head_->Next(0); }

   private:
    const SkipList* list_;
    typename SkipList::Node* node_ = nullptr;
  };

 private:
  static constexpr int kMaxHeight = 12;
  static constexpr unsigned kBranching = 4;

  struct Node {
    explicit Node(const Key& k) : key(k) {}

    Node* Next(int n) { return next[n].load(std::memory_order_acquire); }
    void SetNext(int n, Node* x) { next[n].store(x, std::memory_order_release); }
    Node* NoBarrierNext(int n) { return next[n].load(std::memory_order_relaxed); }
    void NoBarrierSetNext(int n, Node* x) { next[n].store(x, std::memory_order_relaxed); }

    const Key key;
    // Over-allocated to the node's height; next[0] is the bottom level.
    std::atomic<Node*> next[1];
  };

  Node* NewNode(const Key& key, int height) {
    char* mem = arena_->AllocateAligned(sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
    return new (mem) Node(key);
  }

  int RandomHeight() {
    int height = 1;
    while (height < kMaxHeight && NextRandom() % kBranching == 0) {
      ++height;
    }
    return height;
  }

  uint32_t NextRandom() {
    // xorshift32: the writer is single-threaded, so plain state is fine.
    rnd_ ^= rnd_ << 13;
    rnd_ ^= rnd_ >> 17;
    rnd_ ^= rnd_ << 5;
    return rnd_;
  }

  // FindGreaterOrEqual returns the first node >= key, filling prev[level] with the last
  // node < key at each level when prev is non-null.
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = max_height_.load(std::memory_order_relaxed) - 1;
    while (true) {
      Node* next = x->Next(level);
      if (next != nullptr && cmp_(next->key, key) < 0) {
        x = next;
      } else {
        if (prev != nullptr) {
          prev[level] = x;
        }
        if (level == 0) {
          return next;
        }
        --level;
      }
    }
  }

  Comparator const cmp_;
  Arena* const arena_;
  Node* const head_;
  std::atomic<int> max_height_{1};
  uint32_t rnd_ = 0xdeadbeef;
};

}  // namespace kv::storage

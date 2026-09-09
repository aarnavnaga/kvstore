#include "storage/iterator.h"

#include <utility>

namespace kv::storage {
namespace {

class MergingIterator final : public Iterator {
 public:
  explicit MergingIterator(std::vector<std::unique_ptr<Iterator>> children)
      : children_(std::move(children)) {}

  [[nodiscard]] bool Valid() const override { return current_ != nullptr; }

  void SeekToFirst() override {
    for (auto& c : children_) {
      c->SeekToFirst();
    }
    FindSmallest();
  }

  void Seek(std::string_view target) override {
    for (auto& c : children_) {
      c->Seek(target);
    }
    FindSmallest();
  }

  void Next() override {
    // Advance every child positioned at the current key, not just the winner, so the
    // shadowed older versions are skipped.
    const std::string key(current_->key());
    for (auto& c : children_) {
      if (c->Valid() && c->key() == key) {
        c->Next();
      }
    }
    FindSmallest();
  }

  [[nodiscard]] std::string_view key() const override { return current_->key(); }
  [[nodiscard]] std::string_view value() const override { return current_->value(); }
  [[nodiscard]] ValueType type() const override { return current_->type(); }

  [[nodiscard]] Status status() const override {
    for (const auto& c : children_) {
      Status s = c->status();
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }

 private:
  // FindSmallest picks the child with the smallest key; ties go to the earliest
  // (newest) child because the comparison is strict.
  void FindSmallest() {
    current_ = nullptr;
    for (auto& c : children_) {
      if (c->Valid() && (current_ == nullptr || c->key() < current_->key())) {
        current_ = c.get();
      }
    }
  }

  std::vector<std::unique_ptr<Iterator>> children_;
  Iterator* current_ = nullptr;
};

class LiveIterator final : public Iterator {
 public:
  explicit LiveIterator(std::unique_ptr<Iterator> internal) : it_(std::move(internal)) {}

  [[nodiscard]] bool Valid() const override { return it_->Valid(); }
  void SeekToFirst() override {
    it_->SeekToFirst();
    SkipTombstones();
  }
  void Seek(std::string_view target) override {
    it_->Seek(target);
    SkipTombstones();
  }
  void Next() override {
    it_->Next();
    SkipTombstones();
  }
  [[nodiscard]] std::string_view key() const override { return it_->key(); }
  [[nodiscard]] std::string_view value() const override { return it_->value(); }
  [[nodiscard]] ValueType type() const override { return ValueType::kValue; }
  [[nodiscard]] Status status() const override { return it_->status(); }

 private:
  void SkipTombstones() {
    while (it_->Valid() && it_->type() == ValueType::kDeletion) {
      it_->Next();
    }
  }

  std::unique_ptr<Iterator> it_;
};

}  // namespace

std::unique_ptr<Iterator> NewMergingIterator(std::vector<std::unique_ptr<Iterator>> children) {
  return std::make_unique<MergingIterator>(std::move(children));
}

std::unique_ptr<Iterator> NewLiveIterator(std::unique_ptr<Iterator> internal) {
  return std::make_unique<LiveIterator>(std::move(internal));
}

}  // namespace kv::storage

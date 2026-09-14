#include <algorithm>
#include <cassert>

#include "raft/storage.h"

namespace kv::raft {

Term MemoryLogStorage::TermAt(Index i) const {
  if (i == snapshot_.index) {
    return snapshot_.term;
  }
  if (i < snapshot_.index || i > LastIndex()) {
    return 0;
  }
  return entries_[i - snapshot_.index - 1].term;
}

void MemoryLogStorage::GetEntries(Index lo, Index hi, size_t max_bytes,
                                  std::vector<Entry>* out) const {
  lo = std::max(lo, FirstIndex());
  hi = std::min(hi, LastIndex() + 1);
  size_t bytes = 0;
  for (Index i = lo; i < hi; ++i) {
    const Entry& e = entries_[i - snapshot_.index - 1];
    if (i > lo && bytes + e.data.size() > max_bytes) {
      break;
    }
    bytes += e.data.size();
    out->push_back(e);
  }
}

Status MemoryLogStorage::Append(const std::vector<Entry>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }
  const Index first = entries.front().index;
  if (first <= snapshot_.index || first > LastIndex() + 1) {
    return Status::InvalidArgument("append leaves a gap or rewrites the snapshot");
  }
  entries_.resize(first - snapshot_.index - 1);
  for (const Entry& e : entries) {
    assert(e.index == LastIndex() + 1);
    entries_.push_back(e);
  }
  return Status::OK();
}

Status MemoryLogStorage::Compact(const SnapshotMeta& meta) {
  if (meta.index <= snapshot_.index) {
    return Status::OK();
  }
  if (meta.index > LastIndex()) {
    return Status::InvalidArgument("compact past the end of the log");
  }
  entries_.erase(entries_.begin(),
                 entries_.begin() + static_cast<std::ptrdiff_t>(meta.index - snapshot_.index));
  snapshot_ = meta;
  return Status::OK();
}

Status MemoryLogStorage::ResetToSnapshot(const SnapshotMeta& meta) {
  entries_.clear();
  snapshot_ = meta;
  return Status::OK();
}

}  // namespace kv::raft

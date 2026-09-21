#pragma once

#include <vector>

#include "common/status.h"
#include "raft/types.h"

namespace kv::raft {

// LogStorage is the durable half of a Raft node: hard state, the log, and the position of
// the latest snapshot. Every mutating call must be durable when it returns. The node
// relies on that to persist its term and vote before answering a vote request, and its
// log before acknowledging an append.
//
// The log holds entries (snapshot.index, LastIndex()]. Entries at or below the snapshot
// index have been compacted away; only their last term is remembered.
class LogStorage {
 public:
  virtual ~LogStorage() = default;

  [[nodiscard]] virtual HardState LoadHardState() const = 0;
  virtual Status SaveHardState(const HardState& hs) = 0;

  [[nodiscard]] virtual Index FirstIndex() const = 0;  // snapshot index + 1
  [[nodiscard]] virtual Index LastIndex() const = 0;
  // TermAt returns the term of entry i for snapshot index <= i <= LastIndex(), and 0
  // for i == 0.
  [[nodiscard]] virtual Term TermAt(Index i) const = 0;
  // GetEntries appends entries in [lo, hi) to out, stopping once max_bytes of entry data
  // has been gathered (but always returning at least one entry if lo < hi).
  virtual void GetEntries(Index lo, Index hi, size_t max_bytes, std::vector<Entry>* out) const = 0;

  // Append writes contiguous entries. The first must be at most LastIndex()+1; any
  // existing entries at or after it are discarded first (a follower overwriting a
  // conflicting suffix).
  virtual Status Append(const std::vector<Entry>& entries) = 0;

  [[nodiscard]] virtual SnapshotMeta Snapshot() const = 0;
  // Compact records that a snapshot now covers everything up to meta.index and drops
  // those entries. meta.index must be <= LastIndex().
  virtual Status Compact(const SnapshotMeta& meta) = 0;
  // ResetToSnapshot discards the whole log because a snapshot from the leader replaced
  // it; afterwards LastIndex() == meta.index.
  virtual Status ResetToSnapshot(const SnapshotMeta& meta) = 0;
};

// MemoryLogStorage keeps everything in memory. The simulator treats it as the durable
// state that survives a simulated crash.
class MemoryLogStorage final : public LogStorage {
 public:
  [[nodiscard]] HardState LoadHardState() const override { return hard_state_; }
  Status SaveHardState(const HardState& hs) override {
    hard_state_ = hs;
    return Status::OK();
  }

  [[nodiscard]] Index FirstIndex() const override { return snapshot_.index + 1; }
  [[nodiscard]] Index LastIndex() const override { return snapshot_.index + entries_.size(); }
  [[nodiscard]] Term TermAt(Index i) const override;
  void GetEntries(Index lo, Index hi, size_t max_bytes, std::vector<Entry>* out) const override;
  Status Append(const std::vector<Entry>& entries) override;

  [[nodiscard]] SnapshotMeta Snapshot() const override { return snapshot_; }
  Status Compact(const SnapshotMeta& meta) override;
  Status ResetToSnapshot(const SnapshotMeta& meta) override;

 private:
  HardState hard_state_;
  SnapshotMeta snapshot_;
  std::vector<Entry> entries_;  // entries_[0] has index snapshot_.index + 1
};

}  // namespace kv::raft

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/file.h"
#include "raft/storage.h"

namespace kv::raft {

// FileLogStorage persists a Raft log in a directory:
//
//   HARDSTATE   term and vote, replaced atomically (write temp, fsync, rename)
//   SNAPSHOT    index and term of the latest snapshot, replaced atomically
//   raft.log    append-only CRC-framed records, one entry each
//
// Appends are one write() and one fdatasync for the whole batch. Live entries are also
// kept in memory, because the log is bounded by snapshotting and every read of it (to
// replicate or apply) is then a vector lookup. Truncating a conflicting suffix and
// compacting after a snapshot are rare, and rewrite the file atomically.
class FileLogStorage final : public LogStorage {
 public:
  static Status Open(const std::string& dir, std::unique_ptr<FileLogStorage>* out);

  [[nodiscard]] HardState LoadHardState() const override { return hard_state_; }
  Status SaveHardState(const HardState& hs) override;

  [[nodiscard]] Index FirstIndex() const override { return snapshot_.index + 1; }
  [[nodiscard]] Index LastIndex() const override { return snapshot_.index + entries_.size(); }
  [[nodiscard]] Term TermAt(Index i) const override;
  void GetEntries(Index lo, Index hi, size_t max_bytes, std::vector<Entry>* out) const override;
  Status Append(const std::vector<Entry>& entries) override;

  [[nodiscard]] SnapshotMeta Snapshot() const override { return snapshot_; }
  Status Compact(const SnapshotMeta& meta) override;
  Status ResetToSnapshot(const SnapshotMeta& meta) override;

  [[nodiscard]] uint64_t log_file_bytes() const { return log_ ? log_->size() : 0; }
  [[nodiscard]] uint64_t appends() const { return appends_; }
  [[nodiscard]] uint64_t sync_nanos() const { return sync_nanos_; }

 private:
  explicit FileLogStorage(std::string dir) : dir_(std::move(dir)) {}

  Status Load();
  // RewriteLog replaces raft.log with exactly the entries in entries_.
  Status RewriteLog();
  Status SaveSnapshotMeta(const SnapshotMeta& meta);

  std::string dir_;
  HardState hard_state_;
  SnapshotMeta snapshot_;
  std::vector<Entry> entries_;  // entries_[0] has index snapshot_.index + 1
  std::unique_ptr<AppendFile> log_;
  uint64_t appends_ = 0;     // fsynced appends
  uint64_t sync_nanos_ = 0;  // time spent in those fsyncs
};

}  // namespace kv::raft

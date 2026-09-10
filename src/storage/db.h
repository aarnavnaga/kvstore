#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common/status.h"
#include "storage/iterator.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"

namespace kv::storage {

struct Options {
  // Memtable size that triggers a flush to an SSTable.
  size_t write_buffer_size = 4 << 20;
  TableOptions table;
  // sync makes every acknowledged write durable: the WAL is fsynced before Write
  // returns. Concurrent writers share one fsync (group commit).
  bool sync = true;
  // group_commit lets the writer at the head of the queue log the batches of writers
  // queued behind it with a single fsync. Off means one fsync per write, which exists
  // only so the benchmark can measure the difference.
  bool group_commit = true;
  size_t max_group_bytes = 1 << 20;

  // Size-tiered compaction: merge a run of at least compaction_min_tables tables (and at
  // most compaction_max_tables) that are adjacent in age and within
  // [bucket_low, bucket_high] x the run's average size.
  bool background_compaction = true;
  int compaction_min_tables = 4;
  int compaction_max_tables = 16;
  double bucket_low = 0.5;
  double bucket_high = 1.5;
  // Compact everything when tombstones make up this fraction of all table entries, so
  // space freed by mass deletes is reclaimed even when table sizes do not line up.
  double tombstone_ratio = 0.3;
};

struct DBStats {
  uint64_t num_tables = 0;
  uint64_t table_bytes = 0;
  uint64_t memtable_bytes = 0;
  uint64_t flushes = 0;
  uint64_t wal_syncs = 0;
  uint64_t writes = 0;
  uint64_t compactions = 0;
  uint64_t compaction_bytes_read = 0;
  uint64_t compaction_bytes_written = 0;
};

// DB is a single-node LSM-tree key-value store.
//
// Writes go to the WAL, then the memtable. A full memtable becomes immutable and a
// background thread flushes it to an SSTable. Reads check the memtable, the immutable
// memtable, then SSTables newest-first. Reads take the mutex only long enough to copy
// three shared_ptrs, then search without it.
class DB {
 public:
  static Status Open(const Options& options, const std::string& dir, std::unique_ptr<DB>* out);
  ~DB();

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  // Write applies batch atomically: after a crash all of it is visible or none of it.
  Status Write(WriteBatch* batch);
  // Get returns NotFound if key is absent or deleted.
  Status Get(std::string_view key, std::string* value);

  // NewIterator returns an iterator over live keys as of now. Later writes may or may
  // not be visible to it; the SSTables it reads stay alive until it is destroyed.
  std::unique_ptr<Iterator> NewIterator();

  // Flush forces the current memtable to an SSTable and waits for it.
  Status Flush();

  // CompactAll flushes the memtable, then merges every table into one, dropping
  // tombstones and shadowed values. Blocks until done.
  Status CompactAll();

  DBStats GetStats();
  [[nodiscard]] const std::string& dir() const { return dir_; }

 private:
  struct Version {
    std::vector<std::shared_ptr<Table>> tables;  // newest first
  };
  struct Writer;
  // Compaction describes a run of tables, adjacent in age, to merge into one.
  struct Compaction {
    std::vector<std::shared_ptr<Table>> inputs;  // newest first
    // Only a run that includes the oldest table may drop tombstones: anywhere else, an
    // older table outside the run could still hold the value the tombstone shadows.
    bool drop_tombstones = false;
  };

  DB(const Options& options, std::string dir);

  Status Recover();
  Status ReplayLog(uint64_t number, MemTable* mem, uint64_t* max_seq);
  Status NewLogFile(std::unique_ptr<WalWriter>* wal, uint64_t* number);
  // WriteTable writes the entries of iter to a new SSTable file and opens it.
  Status WriteTable(Iterator* iter, uint64_t number, std::shared_ptr<Table>* table);
  Status InstallManifest(const Version& v, uint64_t log_number);
  // LiveLogNumber is the oldest WAL still holding writes that are not in a table.
  [[nodiscard]] uint64_t LiveLogNumber() const { return imm_ ? imm_wal_number_ : wal_number_; }
  void RemoveObsoleteFiles();

  Status WriteInternal(WriteBatch* batch, bool force_flush);
  Status MakeRoomForWrite(std::unique_lock<std::mutex>& lk, bool force);
  WriteBatch* BuildBatchGroup(Writer** last);

  void BackgroundLoop();
  void FlushImmutable(std::unique_lock<std::mutex>& lk);

  void CompactionLoop();
  // PickCompaction chooses the next run to merge, or returns false if none is due.
  bool PickCompaction(bool full, Compaction* c) const;
  Status RunCompaction(std::unique_lock<std::mutex>& lk, const Compaction& c);

  const Options options_;
  const std::string dir_;
  int lock_fd_ = -1;

  std::mutex mu_;
  std::condition_variable bg_cv_;    // wakes the background thread
  std::condition_variable done_cv_;  // signals a finished flush or compaction
  std::condition_variable compact_cv_;
  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;
  std::shared_ptr<const Version> version_;
  std::unique_ptr<WalWriter> wal_;
  uint64_t wal_number_ = 0;
  uint64_t imm_wal_number_ = 0;
  uint64_t next_file_number_ = 1;
  uint64_t last_seq_ = 0;
  std::deque<Writer*> writers_;
  std::set<uint64_t> pending_outputs_;  // table numbers being written, not yet live
  WriteBatch group_batch_;
  Status bg_error_;
  bool shutting_down_ = false;
  bool compacting_ = false;
  bool full_compaction_requested_ = false;
  DBStats stats_;
  std::thread bg_thread_;
  std::thread compact_thread_;
};

}  // namespace kv::storage

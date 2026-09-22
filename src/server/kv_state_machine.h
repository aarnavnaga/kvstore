#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "net/frame.h"
#include "raft/types.h"
#include "storage/db.h"

namespace kv::server {

// Command is a client operation as stored in a Raft log entry:
//   u8 op | varint client_id | varint seq | varint key_len | key | varint value_len | value
struct Command {
  net::MsgType op = net::MsgType::kGet;
  uint64_t client_id = 0;
  uint64_t seq = 0;
  std::string key;
  std::string value;
};

std::string EncodeCommand(const Command& c);
bool DecodeCommand(std::string_view in, Command* c);

struct ApplyResult {
  Status status;
  std::string value;  // GET result
};

// Keys starting with this prefix are reserved for the state machine's own bookkeeping
// and rejected from clients.
inline constexpr std::string_view kReservedPrefix = "\xff\xff";

// KvStateMachine applies committed Raft entries to the local LSM tree.
//
// Each Apply call is one atomic WriteBatch holding the data, the updated client sessions
// and the new applied index. After a crash the store's contents and its applied index
// therefore always agree, and recovery simply resumes from that index. The DB runs with
// sync=false: the Raft log is already durable, so anything lost from an unsynced WAL is
// re-applied from it (and before the log is ever compacted, the DB is flushed).
class KvStateMachine {
 public:
  static Status Open(const storage::Options& options, const std::string& dir,
                     std::unique_ptr<KvStateMachine>* out);

  // Apply applies entries in log order. results receives one result per entry.
  Status Apply(const std::vector<raft::Entry>& entries, std::vector<ApplyResult>* results);

  // Get reads the current state directly (used once a read is known to be linearizable).
  Status Get(std::string_view key, std::string* value);

  // Sync makes everything applied so far durable (flushes the memtable). Required
  // before the Raft log is compacted past the applied index, since the DB runs unsynced.
  Status Sync();

  // CreateSnapshot serializes the whole state, bookkeeping keys included, as of the
  // applied index.
  Status CreateSnapshot(raft::Snapshot* out);

  // RestoreSnapshot replaces the whole state with snap. The new state is built in a
  // side directory and swapped in with renames, so a crash leaves the old state or the
  // new one, never a mix; Open finishes or discards an interrupted swap.
  Status RestoreSnapshot(const raft::Snapshot& snap);

  [[nodiscard]] raft::Index applied_index() const { return applied_; }
  [[nodiscard]] raft::Term applied_term() const { return applied_term_; }
  // apply_hash is a running hash of every entry applied, in order. Two replicas at the
  // same applied index have equal hashes iff they applied the same log.
  [[nodiscard]] uint64_t apply_hash() const { return hash_; }
  storage::DB* db() { return db_.get(); }

 private:
  KvStateMachine(std::unique_ptr<storage::DB> db, storage::Options options, std::string dir)
      : db_(std::move(db)), options_(options), dir_(std::move(dir)) {}
  Status LoadMeta();
  Status SessionSeq(uint64_t client_id, uint64_t* seq);

  std::unique_ptr<storage::DB> db_;
  storage::Options options_;
  std::string dir_;
  raft::Index applied_ = 0;
  raft::Term applied_term_ = 0;
  uint64_t hash_ = 0;
  std::map<uint64_t, uint64_t> sessions_;  // client_id -> last applied seq (cache)
};

}  // namespace kv::server

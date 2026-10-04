#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"

namespace kv::raft {

using NodeId = uint64_t;
using Term = uint64_t;
using Index = uint64_t;

constexpr NodeId kNoNode = 0;

enum class EntryType : uint8_t {
  kNormal = 0,  // client command for the state machine
  kNoop = 1,    // appended by a new leader to commit entries from earlier terms
};

struct Entry {
  Term term = 0;
  Index index = 0;
  EntryType type = EntryType::kNormal;
  std::string data;
};

// HardState is the Raft state that must reach stable storage before the node answers any
// RPC (Figure 2, "persistent state on all servers"; the log is persisted separately).
struct HardState {
  Term term = 0;
  NodeId vote = kNoNode;
  // joining survives restarts: a wiped node that crashes again before catching up must
  // not come back as a voter just because nobody passed --join a second time.
  bool joining = false;

  bool operator==(const HardState&) const = default;
};

struct SnapshotMeta {
  Index index = 0;  // last entry the snapshot covers
  Term term = 0;    // that entry's term
};

// Snapshot is the state machine's full state as of meta.index, as an opaque blob.
struct Snapshot {
  SnapshotMeta meta;
  std::string data;
};

enum class MsgKind : uint8_t {
  kVoteRequest = 1,
  kVoteResponse = 2,
  kAppendRequest = 3,
  kAppendResponse = 4,
  kSnapshotRequest = 5,
  kSnapshotResponse = 6,
};

// Message is every RPC between Raft nodes. Fields not used by a kind stay zero.
struct Message {
  MsgKind kind = MsgKind::kVoteRequest;
  NodeId from = kNoNode;
  NodeId to = kNoNode;
  Term term = 0;

  // Vote request: the candidate's last log position. pre_vote marks a PreVote round,
  // which asks "would you vote for me?" without anyone changing term.
  Index last_log_index = 0;
  Term last_log_term = 0;
  bool pre_vote = false;

  // Vote response.
  bool granted = false;

  // Append request: entries follow (prev_log_index, prev_log_term).
  Index prev_log_index = 0;
  Term prev_log_term = 0;
  std::vector<Entry> entries;
  Index commit = 0;

  // Append response. On success match_index is the follower's last matching index; on
  // rejection reject_hint is where the leader should retry from.
  bool success = false;
  Index match_index = 0;
  Index reject_hint = 0;

  // ReadIndex: a leader tags a heartbeat round with read_ctx; followers echo it back.
  uint64_t read_ctx = 0;

  // Snapshot request: one chunk of a snapshot, at byte offset; done marks the last chunk.
  SnapshotMeta snapshot;
  uint64_t offset = 0;
  std::string data;
  bool done = false;
};

const char* MsgKindName(MsgKind kind);

std::string EncodeMessage(const Message& m);
bool DecodeMessage(std::string_view in, Message* m);

std::string EncodeEntry(const Entry& e);
bool DecodeEntry(std::string_view* in, Entry* e);

}  // namespace kv::raft

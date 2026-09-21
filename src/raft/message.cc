#include "common/coding.h"
#include "raft/types.h"

namespace kv::raft {

const char* MsgKindName(MsgKind kind) {
  switch (kind) {
    case MsgKind::kVoteRequest:
      return "VoteRequest";
    case MsgKind::kVoteResponse:
      return "VoteResponse";
    case MsgKind::kAppendRequest:
      return "AppendRequest";
    case MsgKind::kAppendResponse:
      return "AppendResponse";
    case MsgKind::kSnapshotRequest:
      return "SnapshotRequest";
    case MsgKind::kSnapshotResponse:
      return "SnapshotResponse";
  }
  return "Unknown";
}

std::string EncodeEntry(const Entry& e) {
  std::string out;
  PutVarint64(&out, e.term);
  PutVarint64(&out, e.index);
  out.push_back(static_cast<char>(e.type));
  PutLengthPrefixed(&out, e.data);
  return out;
}

bool DecodeEntry(std::string_view* in, Entry* e) {
  uint8_t type = 0;
  std::string_view data;
  if (!GetVarint64(in, &e->term) || !GetVarint64(in, &e->index) || !GetByte(in, &type) ||
      type > static_cast<uint8_t>(EntryType::kNoop) || !GetLengthPrefixed(in, &data)) {
    return false;
  }
  e->type = static_cast<EntryType>(type);
  e->data.assign(data);
  return true;
}

// Every message uses one layout with all fields present. A few bytes of zero varints per
// heartbeat is cheaper than a kind-specific codec that can drift out of sync.
std::string EncodeMessage(const Message& m) {
  std::string out;
  out.push_back(static_cast<char>(m.kind));
  PutVarint64(&out, m.from);
  PutVarint64(&out, m.to);
  PutVarint64(&out, m.term);
  PutVarint64(&out, m.last_log_index);
  PutVarint64(&out, m.last_log_term);
  const uint8_t flags =
      (m.pre_vote ? 1 : 0) | (m.granted ? 2 : 0) | (m.success ? 4 : 0) | (m.done ? 8 : 0);
  out.push_back(static_cast<char>(flags));
  PutVarint64(&out, m.prev_log_index);
  PutVarint64(&out, m.prev_log_term);
  PutVarint64(&out, m.commit);
  PutVarint64(&out, m.match_index);
  PutVarint64(&out, m.reject_hint);
  PutVarint64(&out, m.read_ctx);
  PutVarint64(&out, m.snapshot.index);
  PutVarint64(&out, m.snapshot.term);
  PutVarint64(&out, m.offset);
  PutLengthPrefixed(&out, m.data);
  PutVarint64(&out, m.entries.size());
  for (const Entry& e : m.entries) {
    out.append(EncodeEntry(e));
  }
  return out;
}

bool DecodeMessage(std::string_view in, Message* m) {
  uint8_t kind = 0;
  uint8_t flags = 0;
  std::string_view data;
  uint64_t count = 0;
  if (!GetByte(&in, &kind) || kind < static_cast<uint8_t>(MsgKind::kVoteRequest) ||
      kind > static_cast<uint8_t>(MsgKind::kSnapshotResponse) || !GetVarint64(&in, &m->from) ||
      !GetVarint64(&in, &m->to) || !GetVarint64(&in, &m->term) ||
      !GetVarint64(&in, &m->last_log_index) || !GetVarint64(&in, &m->last_log_term) ||
      !GetByte(&in, &flags) || !GetVarint64(&in, &m->prev_log_index) ||
      !GetVarint64(&in, &m->prev_log_term) || !GetVarint64(&in, &m->commit) ||
      !GetVarint64(&in, &m->match_index) || !GetVarint64(&in, &m->reject_hint) ||
      !GetVarint64(&in, &m->read_ctx) || !GetVarint64(&in, &m->snapshot.index) ||
      !GetVarint64(&in, &m->snapshot.term) || !GetVarint64(&in, &m->offset) ||
      !GetLengthPrefixed(&in, &data) || !GetVarint64(&in, &count)) {
    return false;
  }
  m->kind = static_cast<MsgKind>(kind);
  m->pre_vote = (flags & 1) != 0;
  m->granted = (flags & 2) != 0;
  m->success = (flags & 4) != 0;
  m->done = (flags & 8) != 0;
  m->data.assign(data);
  // Each entry is at least 4 bytes, which bounds a hostile count before reserving.
  if (count > in.size() / 4 + 1) {
    return false;
  }
  m->entries.clear();
  m->entries.reserve(count);
  for (uint64_t i = 0; i < count; ++i) {
    Entry e;
    if (!DecodeEntry(&in, &e)) {
      return false;
    }
    m->entries.push_back(std::move(e));
  }
  return in.empty();
}

}  // namespace kv::raft

#pragma once

// Client request and response payloads carried inside frames.
//
//   PUT     varint client_id | varint seq | varint key_len | key | varint value_len | value
//   GET     varint key_len | key
//   DELETE  varint client_id | varint seq | varint key_len | key
//   RESPONSE u8 status code | varint len | value or error message | varint len | leader hint
//
// Writes carry (client_id, seq) so the state machine can recognize a retry of a write it
// already applied and answer it without applying it twice. Without that, a client that
// times out and retries could make an old value reappear after a newer one.
//
// Contract: a client_id has at most one write in flight at a time, and seq increases by
// one per new write. The server only remembers each session's highest applied seq, so two
// concurrent writes on one session could commit out of order and the lower one would be
// acknowledged as a "duplicate" without being applied. Concurrent clients use one session
// per in-flight write (the Java SDK's session lanes).
//
// A NotLeader response carries the leader's client address (if known) as the hint, so a
// client can retry there directly instead of probing every node.

#include <string>
#include <string_view>

#include "common/status.h"
#include "net/frame.h"

namespace kv::net {

struct KvRequest {
  MsgType op = MsgType::kGet;
  std::string key;
  std::string value;
  uint64_t client_id = 0;  // writes only; 0 means "no session, never deduplicate"
  uint64_t seq = 0;
};

std::string EncodeKvRequest(const KvRequest& req);
// DecodeKvRequest parses the payload of a PUT, GET or DELETE frame.
bool DecodeKvRequest(MsgType type, std::string_view payload, KvRequest* req);

struct KvResponse {
  Status::Code code = Status::Code::kOk;
  std::string value;  // GET result, or the error message for a failure
  std::string leader_hint;
};

std::string EncodeKvResponse(const KvResponse& resp);
bool DecodeKvResponse(std::string_view payload, KvResponse* resp);

// ResponseFromStatus builds a response carrying s's code and message.
KvResponse ResponseFromStatus(const Status& s);
// StatusFromResponse rebuilds a Status from a non-OK response.
Status StatusFromResponse(const KvResponse& resp);

}  // namespace kv::net

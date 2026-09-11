#include "net/protocol.h"

#include "common/coding.h"

namespace kv::net {

std::string EncodeKvRequest(const KvRequest& req) {
  std::string out;
  if (req.op != MsgType::kGet) {
    PutVarint64(&out, req.client_id);
    PutVarint64(&out, req.seq);
  }
  PutLengthPrefixed(&out, req.key);
  if (req.op == MsgType::kPut) {
    PutLengthPrefixed(&out, req.value);
  }
  return out;
}

bool DecodeKvRequest(MsgType type, std::string_view payload, KvRequest* req) {
  if (type != MsgType::kPut && type != MsgType::kGet && type != MsgType::kDelete) {
    return false;
  }
  std::string_view key;
  std::string_view value;
  uint64_t client_id = 0;
  uint64_t seq = 0;
  if (type != MsgType::kGet &&
      (!GetVarint64(&payload, &client_id) || !GetVarint64(&payload, &seq))) {
    return false;
  }
  if (!GetLengthPrefixed(&payload, &key)) {
    return false;
  }
  if (type == MsgType::kPut && !GetLengthPrefixed(&payload, &value)) {
    return false;
  }
  if (!payload.empty()) {
    return false;  // trailing bytes mean the peer and we disagree on the format
  }
  req->op = type;
  req->key.assign(key);
  req->value.assign(value);
  req->client_id = client_id;
  req->seq = seq;
  return true;
}

std::string EncodeKvResponse(const KvResponse& resp) {
  std::string out;
  out.push_back(static_cast<char>(resp.code));
  PutLengthPrefixed(&out, resp.value);
  PutLengthPrefixed(&out, resp.leader_hint);
  return out;
}

bool DecodeKvResponse(std::string_view payload, KvResponse* resp) {
  uint8_t code = 0;
  std::string_view value;
  std::string_view hint;
  if (!GetByte(&payload, &code) || code > static_cast<uint8_t>(Status::Code::kUnavailable) ||
      !GetLengthPrefixed(&payload, &value) || !GetLengthPrefixed(&payload, &hint) ||
      !payload.empty()) {
    return false;
  }
  resp->code = static_cast<Status::Code>(code);
  resp->value.assign(value);
  resp->leader_hint.assign(hint);
  return true;
}

KvResponse ResponseFromStatus(const Status& s) {
  KvResponse r;
  r.code = s.code();
  r.value = s.message();
  return r;
}

Status StatusFromResponse(const KvResponse& resp) {
  switch (resp.code) {
    case Status::Code::kOk:
      return Status::OK();
    case Status::Code::kNotFound:
      return Status::NotFound(resp.value);
    case Status::Code::kCorruption:
      return Status::Corruption(resp.value);
    case Status::Code::kIOError:
      return Status::IOError(resp.value);
    case Status::Code::kInvalidArgument:
      return Status::InvalidArgument(resp.value);
    case Status::Code::kNotLeader:
      return Status::NotLeader(resp.leader_hint);
    case Status::Code::kTimeout:
      return Status::Timeout(resp.value);
    case Status::Code::kUnavailable:
      return Status::Unavailable(resp.value);
  }
  return Status::Corruption("unknown status code");
}

}  // namespace kv::net

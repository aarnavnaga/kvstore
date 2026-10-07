#include "client/kv_client.h"

#include <algorithm>
#include <random>
#include <thread>

namespace kv::client {
namespace {

using Clock = std::chrono::steady_clock;

uint64_t RandomClientId() {
  std::random_device rd;
  uint64_t id = 0;
  while (id == 0) {
    id = (static_cast<uint64_t>(rd()) << 32) | rd();
  }
  return id;
}

// Retryable reports whether an attempt failed in a way another node, or the same node a
// moment later, might not.
bool Retryable(Status::Code code) {
  return code == Status::Code::kNotLeader || code == Status::Code::kUnavailable ||
         code == Status::Code::kTimeout;
}

}  // namespace

KvClient::KvClient(ClientOptions options)
    : options_(std::move(options)),
      client_id_(options_.client_id != 0 ? options_.client_id : RandomClientId()) {}

Status KvClient::Put(std::string_view key, std::string_view value) {
  net::KvRequest req;
  req.op = net::MsgType::kPut;
  req.key.assign(key);
  req.value.assign(value);
  net::KvResponse resp;
  return Execute(std::move(req), &resp);
}

Status KvClient::Get(std::string_view key, std::string* value) {
  net::KvRequest req;
  req.op = net::MsgType::kGet;
  req.key.assign(key);
  net::KvResponse resp;
  KV_RETURN_IF_ERROR(Execute(std::move(req), &resp));
  *value = std::move(resp.value);
  return Status::OK();
}

Status KvClient::Delete(std::string_view key) {
  net::KvRequest req;
  req.op = net::MsgType::kDelete;
  req.key.assign(key);
  net::KvResponse resp;
  return Execute(std::move(req), &resp);
}

Status KvClient::Ping(const std::string& addr) {
  std::lock_guard lk(mu_);
  net::Frame resp;
  return CallNode(addr, net::MsgType::kPing, {}, &resp);
}

Status KvClient::NodeStatus(const std::string& addr, std::string* out) {
  std::lock_guard lk(mu_);
  net::Frame resp;
  KV_RETURN_IF_ERROR(CallNode(addr, net::MsgType::kStatus, {}, &resp));
  *out = std::move(resp.payload);
  return Status::OK();
}

std::string KvClient::leader() {
  std::lock_guard lk(mu_);
  return leader_;
}

Status KvClient::Execute(net::KvRequest req, net::KvResponse* resp) {
  if (options_.servers.empty()) {
    return Status::InvalidArgument("no servers configured");
  }
  std::lock_guard lk(mu_);
  if (!options_.sticky_leader) {
    leader_.clear();
  }
  if (req.op != net::MsgType::kGet) {
    // One sequence number per logical write, reused by every retry: that is what lets
    // the state machine recognize a duplicate.
    req.client_id = client_id_;
    req.seq = next_seq_++;
  }

  const auto deadline = Clock::now() + options_.operation_timeout;
  auto backoff = options_.backoff_initial;
  Status last = Status::Unavailable("no attempt made");
  while (Clock::now() < deadline) {
    const std::string addr =
        !leader_.empty() ? leader_ : options_.servers[next_server_ % options_.servers.size()];
    last = Attempt(addr, req, resp);
    if (last.ok() || !Retryable(last.code())) {
      return last;
    }
    if (last.IsNotLeader() && !last.message().empty() && last.message() != addr) {
      leader_ = last.message();  // redirect straight to the hinted leader
      continue;
    }
    // No usable hint (election in progress, or the node is unreachable): try the next
    // server after a pause, so a leaderless cluster is not hammered.
    leader_.clear();
    ++next_server_;
    const auto left = std::chrono::duration_cast<net::Millis>(deadline - Clock::now());
    std::this_thread::sleep_for(std::min(backoff, std::max(left, net::Millis(0))));
    backoff = std::min(backoff * 2, options_.backoff_max);
  }
  if (last.code() == Status::Code::kNotLeader) {
    return Status::Unavailable("no leader found before the deadline");
  }
  return last.code() == Status::Code::kTimeout ? last : Status::Timeout(last.ToString());
}

Status KvClient::Attempt(const std::string& addr, const net::KvRequest& req,
                         net::KvResponse* resp) {
  net::Frame frame;
  KV_RETURN_IF_ERROR(CallNode(addr, req.op, net::EncodeKvRequest(req), &frame));
  if (frame.type != net::MsgType::kResponse || !net::DecodeKvResponse(frame.payload, resp)) {
    conns_.erase(addr);
    return Status::Corruption("malformed response from " + addr);
  }
  if (resp->code == Status::Code::kOk) {
    leader_ = addr;
  }
  return net::StatusFromResponse(*resp);
}

Status KvClient::CallNode(const std::string& addr, net::MsgType type, std::string payload,
                          net::Frame* response) {
  auto it = conns_.find(addr);
  if (it == conns_.end()) {
    std::unique_ptr<net::Connection> conn;
    KV_RETURN_IF_ERROR(net::Connection::Dial(addr, options_.request_timeout, &conn));
    it = conns_.emplace(addr, std::move(conn)).first;
  }
  Status s = it->second->Call(type, std::move(payload), response, options_.request_timeout);
  if (!s.ok()) {
    // The stream may hold a late response or be half-written; start fresh next time.
    conns_.erase(it);
    if (!Retryable(s.code())) {
      return Status::Unavailable(s.ToString());
    }
  }
  return s;
}

}  // namespace kv::client

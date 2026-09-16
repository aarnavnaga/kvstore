#include "server/kv_state_machine.h"

#include <optional>

#include "common/coding.h"

namespace kv::server {
namespace {

const std::string kMetaKey = std::string(kReservedPrefix) + "meta";

std::string SessionKey(uint64_t client_id) {
  std::string k(kReservedPrefix);
  k += "sess:";
  PutFixed64(&k, client_id);
  return k;
}

uint64_t HashStep(uint64_t h, const raft::Entry& e) {
  // FNV-1a over (index, term, data), chained.
  auto mix = [&h](const void* p, size_t n) {
    const auto* b = static_cast<const unsigned char*>(p);
    for (size_t i = 0; i < n; ++i) {
      h ^= b[i];
      h *= 0x100000001b3ULL;
    }
  };
  mix(&e.index, sizeof(e.index));
  mix(&e.term, sizeof(e.term));
  mix(e.data.data(), e.data.size());
  return h;
}

}  // namespace

std::string EncodeCommand(const Command& c) {
  std::string out;
  out.push_back(static_cast<char>(c.op));
  PutVarint64(&out, c.client_id);
  PutVarint64(&out, c.seq);
  PutLengthPrefixed(&out, c.key);
  PutLengthPrefixed(&out, c.value);
  return out;
}

bool DecodeCommand(std::string_view in, Command* c) {
  uint8_t op = 0;
  std::string_view key;
  std::string_view value;
  if (!GetByte(&in, &op) || !GetVarint64(&in, &c->client_id) || !GetVarint64(&in, &c->seq) ||
      !GetLengthPrefixed(&in, &key) || !GetLengthPrefixed(&in, &value) || !in.empty()) {
    return false;
  }
  c->op = static_cast<net::MsgType>(op);
  if (c->op != net::MsgType::kPut && c->op != net::MsgType::kGet &&
      c->op != net::MsgType::kDelete) {
    return false;
  }
  c->key.assign(key);
  c->value.assign(value);
  return true;
}

Status KvStateMachine::Open(const storage::Options& options, const std::string& dir,
                            std::unique_ptr<KvStateMachine>* out) {
  std::unique_ptr<storage::DB> db;
  KV_RETURN_IF_ERROR(storage::DB::Open(options, dir, &db));
  std::unique_ptr<KvStateMachine> sm(new KvStateMachine(std::move(db)));
  KV_RETURN_IF_ERROR(sm->LoadMeta());
  *out = std::move(sm);
  return Status::OK();
}

Status KvStateMachine::LoadMeta() {
  std::string meta;
  Status s = db_->Get(kMetaKey, &meta);
  if (s.IsNotFound()) {
    return Status::OK();
  }
  KV_RETURN_IF_ERROR(s);
  std::string_view in(meta);
  if (!GetFixed64(&in, &applied_) || !GetFixed64(&in, &applied_term_) || !GetFixed64(&in, &hash_)) {
    return Status::Corruption("state machine metadata");
  }
  return Status::OK();
}

Status KvStateMachine::SessionSeq(uint64_t client_id, uint64_t* seq) {
  const auto it = sessions_.find(client_id);
  if (it != sessions_.end()) {
    *seq = it->second;
    return Status::OK();
  }
  std::string v;
  Status s = db_->Get(SessionKey(client_id), &v);
  if (s.IsNotFound()) {
    *seq = 0;
  } else {
    KV_RETURN_IF_ERROR(s);
    *seq = DecodeFixed64(v.data());
  }
  sessions_[client_id] = *seq;
  return Status::OK();
}

Status KvStateMachine::Apply(const std::vector<raft::Entry>& entries,
                             std::vector<ApplyResult>* results) {
  results->clear();
  results->resize(entries.size());
  if (entries.empty()) {
    return Status::OK();
  }
  storage::WriteBatch batch;
  // Writes earlier in this batch are not in the DB yet; reads later in the batch must
  // still see them, in log order.
  std::map<std::string, std::optional<std::string>, std::less<>> overlay;

  uint64_t hash = hash_;
  for (size_t i = 0; i < entries.size(); ++i) {
    const raft::Entry& e = entries[i];
    hash = HashStep(hash, e);
    if (e.type != raft::EntryType::kNormal) {
      continue;
    }
    Command cmd;
    if (!DecodeCommand(e.data, &cmd)) {
      // Every replica rejects the same bytes the same way, so this stays deterministic.
      (*results)[i].status = Status::Corruption("undecodable command");
      continue;
    }
    if (cmd.op == net::MsgType::kGet) {
      const auto it = overlay.find(cmd.key);
      if (it != overlay.end()) {
        (*results)[i].status = it->second ? Status::OK() : Status::NotFound();
        (*results)[i].value = it->second.value_or("");
      } else {
        (*results)[i].status = db_->Get(cmd.key, &(*results)[i].value);
      }
      continue;
    }
    if (cmd.client_id != 0) {
      uint64_t last = 0;
      KV_RETURN_IF_ERROR(SessionSeq(cmd.client_id, &last));
      if (cmd.seq <= last) {
        // A retry of a write this client already got applied: acknowledge it again
        // without applying it twice.
        continue;
      }
      sessions_[cmd.client_id] = cmd.seq;
      std::string v;
      PutFixed64(&v, cmd.seq);
      batch.Put(SessionKey(cmd.client_id), v);
    }
    if (cmd.op == net::MsgType::kPut) {
      batch.Put(cmd.key, cmd.value);
      overlay[cmd.key] = cmd.value;
    } else {
      batch.Delete(cmd.key);
      overlay[cmd.key] = std::nullopt;
    }
  }

  std::string meta;
  PutFixed64(&meta, entries.back().index);
  PutFixed64(&meta, entries.back().term);
  PutFixed64(&meta, hash);
  batch.Put(kMetaKey, meta);
  Status s = db_->Write(&batch);
  if (!s.ok()) {
    sessions_.clear();  // the cache may now be ahead of the DB
    return s;
  }
  applied_ = entries.back().index;
  applied_term_ = entries.back().term;
  hash_ = hash;
  return Status::OK();
}

Status KvStateMachine::Get(std::string_view key, std::string* value) {
  return db_->Get(key, value);
}

}  // namespace kv::server

#include "raft/file_storage.h"

#include <algorithm>
#include <cstdio>

#include "common/coding.h"
#include "common/crc32c.h"
#include "storage/wal.h"

namespace kv::raft {
namespace {

constexpr char kHardState[] = "HARDSTATE";
constexpr char kSnapshot[] = "SNAPSHOT";
constexpr char kLog[] = "raft.log";

// Small fixed records: fixed64 a | fixed64 b | crc32c.
std::string EncodePair(uint64_t a, uint64_t b) {
  std::string out;
  PutFixed64(&out, a);
  PutFixed64(&out, b);
  PutFixed32(&out, crc32c::Value(out));
  return out;
}

Status DecodePair(const std::string& data, uint64_t* a, uint64_t* b) {
  if (data.size() != 20 || crc32c::Value(data.data(), 16) != DecodeFixed32(data.data() + 16)) {
    return Status::Corruption("bad checksum");
  }
  *a = DecodeFixed64(data.data());
  *b = DecodeFixed64(data.data() + 8);
  return Status::OK();
}

Status ReadPair(const std::string& path, uint64_t* a, uint64_t* b) {
  std::string data;
  Status s = ReadFileToString(path, &data);
  if (s.IsNotFound()) {
    *a = 0;
    *b = 0;
    return Status::OK();
  }
  KV_RETURN_IF_ERROR(s);
  s = DecodePair(data, a, b);
  if (!s.ok()) {
    return Status::Corruption(path + ": " + s.message());
  }
  return Status::OK();
}

}  // namespace

Status FileLogStorage::Open(const std::string& dir, std::unique_ptr<FileLogStorage>* out) {
  KV_RETURN_IF_ERROR(CreateDirIfMissing(dir));
  std::unique_ptr<FileLogStorage> s(new FileLogStorage(dir));
  KV_RETURN_IF_ERROR(s->Load());
  *out = std::move(s);
  return Status::OK();
}

Status FileLogStorage::Load() {
  KV_RETURN_IF_ERROR(ReadPair(JoinPath(dir_, kHardState), &hard_state_.term, &hard_state_.vote));
  KV_RETURN_IF_ERROR(ReadPair(JoinPath(dir_, kSnapshot), &snapshot_.index, &snapshot_.term));

  std::string contents;
  const std::string path = JoinPath(dir_, kLog);
  Status s = ReadFileToString(path, &contents);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  storage::WalReader reader(contents);
  std::string_view record;
  size_t good_bytes = 0;
  bool skipped = false;
  while (reader.Next(&record)) {
    Entry e;
    std::string_view in = record;
    if (!DecodeEntry(&in, &e)) {
      return Status::Corruption(path + ": undecodable entry with a valid checksum");
    }
    good_bytes += 8 + record.size();
    if (e.index <= snapshot_.index) {
      skipped = true;  // covered by the snapshot; the file is rewritten below
      continue;
    }
    if (e.index != LastIndex() + 1) {
      return Status::Corruption(path + ": entry " + std::to_string(e.index) + " out of order");
    }
    entries_.push_back(std::move(e));
  }
  if (reader.tail_dropped() > 0 || good_bytes != contents.size() || skipped) {
    // Drop a torn tail (or entries the snapshot already covers) so appends continue
    // from a clean end of file.
    if (reader.tail_dropped() > 0) {
      std::fprintf(stderr, "raft: dropped %zu bytes of torn tail from %s\n", reader.tail_dropped(),
                   path.c_str());
    }
    return RewriteLog();
  }
  return AppendFile::Open(path, &log_);
}

Status FileLogStorage::SaveHardState(const HardState& hs) {
  if (hs == hard_state_) {
    return Status::OK();
  }
  KV_RETURN_IF_ERROR(WriteFileAtomic(JoinPath(dir_, kHardState), EncodePair(hs.term, hs.vote)));
  hard_state_ = hs;
  return Status::OK();
}

Status FileLogStorage::SaveSnapshotMeta(const SnapshotMeta& meta) {
  return WriteFileAtomic(JoinPath(dir_, kSnapshot), EncodePair(meta.index, meta.term));
}

Term FileLogStorage::TermAt(Index i) const {
  if (i == snapshot_.index) {
    return snapshot_.term;
  }
  if (i < snapshot_.index || i > LastIndex()) {
    return 0;
  }
  return entries_[i - snapshot_.index - 1].term;
}

void FileLogStorage::GetEntries(Index lo, Index hi, size_t max_bytes,
                                std::vector<Entry>* out) const {
  lo = std::max(lo, FirstIndex());
  hi = std::min(hi, LastIndex() + 1);
  size_t bytes = 0;
  for (Index i = lo; i < hi; ++i) {
    const Entry& e = entries_[i - snapshot_.index - 1];
    if (i > lo && bytes + e.data.size() > max_bytes) {
      break;
    }
    bytes += e.data.size();
    out->push_back(e);
  }
}

Status FileLogStorage::Append(const std::vector<Entry>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }
  const Index first = entries.front().index;
  if (first <= snapshot_.index || first > LastIndex() + 1) {
    return Status::InvalidArgument("append leaves a gap or rewrites the snapshot");
  }
  if (first <= LastIndex()) {
    // A follower replacing a conflicting suffix: rewrite the file without it first, so
    // a crash can never leave the old suffix followed by the new entries.
    entries_.resize(first - snapshot_.index - 1);
    KV_RETURN_IF_ERROR(RewriteLog());
  }
  std::string buf;
  for (const Entry& e : entries) {
    buf.append(storage::EncodeRecord(EncodeEntry(e)));
  }
  KV_RETURN_IF_ERROR(log_->Append(buf));
  KV_RETURN_IF_ERROR(log_->Sync());
  entries_.insert(entries_.end(), entries.begin(), entries.end());
  return Status::OK();
}

Status FileLogStorage::Compact(const SnapshotMeta& meta) {
  if (meta.index <= snapshot_.index) {
    return Status::OK();
  }
  if (meta.index > LastIndex()) {
    return Status::InvalidArgument("compact past the end of the log");
  }
  // Record the snapshot first: if we crash before the rewrite, Load skips the entries
  // the snapshot covers and rewrites the file then.
  KV_RETURN_IF_ERROR(SaveSnapshotMeta(meta));
  entries_.erase(entries_.begin(),
                 entries_.begin() + static_cast<std::ptrdiff_t>(meta.index - snapshot_.index));
  snapshot_ = meta;
  return RewriteLog();
}

Status FileLogStorage::ResetToSnapshot(const SnapshotMeta& meta) {
  KV_RETURN_IF_ERROR(SaveSnapshotMeta(meta));
  entries_.clear();
  snapshot_ = meta;
  return RewriteLog();
}

Status FileLogStorage::RewriteLog() {
  std::string buf;
  for (const Entry& e : entries_) {
    buf.append(storage::EncodeRecord(EncodeEntry(e)));
  }
  log_.reset();
  const std::string path = JoinPath(dir_, kLog);
  KV_RETURN_IF_ERROR(WriteFileAtomic(path, buf));
  return AppendFile::Open(path, &log_);
}

}  // namespace kv::raft

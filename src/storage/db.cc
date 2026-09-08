#include "storage/db.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>
#include <cstdio>

#include "common/file.h"
#include "storage/manifest.h"

namespace kv::storage {
namespace {

enum class FileType { kLog, kTable, kOther };

std::string LogFileName(const std::string& dir, uint64_t number) {
  char name[32];
  std::snprintf(name, sizeof(name), "%06llu.log", static_cast<unsigned long long>(number));
  return JoinPath(dir, name);
}

// ParseFileName recognizes "<number>.log" and "<number>.sst".
FileType ParseFileName(std::string_view name, uint64_t* number) {
  const size_t dot = name.find('.');
  if (dot == std::string_view::npos || dot == 0) {
    return FileType::kOther;
  }
  const auto [ptr, ec] = std::from_chars(name.data(), name.data() + dot, *number);
  if (ec != std::errc() || ptr != name.data() + dot) {
    return FileType::kOther;
  }
  const std::string_view ext = name.substr(dot);
  if (ext == ".log") {
    return FileType::kLog;
  }
  if (ext == ".sst") {
    return FileType::kTable;
  }
  return FileType::kOther;
}

class MemInserter final : public WriteBatch::Handler {
 public:
  MemInserter(uint64_t seq, MemTable* mem) : seq_(seq), mem_(mem) {}
  void Put(std::string_view key, std::string_view value) override {
    mem_->Add(seq_++, ValueType::kValue, key, value);
  }
  void Delete(std::string_view key) override { mem_->Add(seq_++, ValueType::kDeletion, key, {}); }

 private:
  uint64_t seq_;
  MemTable* mem_;
};

Status InsertInto(const WriteBatch& batch, MemTable* mem) {
  MemInserter inserter(batch.Sequence(), mem);
  return batch.Iterate(&inserter);
}

// Pinned wraps an iterator together with the object it reads from, keeping a memtable
// alive for as long as an iterator over it exists.
class Pinned final : public Iterator {
 public:
  Pinned(std::shared_ptr<const void> owner, std::unique_ptr<Iterator> it)
      : owner_(std::move(owner)), it_(std::move(it)) {}
  [[nodiscard]] bool Valid() const override { return it_->Valid(); }
  void SeekToFirst() override { it_->SeekToFirst(); }
  void Seek(std::string_view t) override { it_->Seek(t); }
  void Next() override { it_->Next(); }
  [[nodiscard]] std::string_view key() const override { return it_->key(); }
  [[nodiscard]] std::string_view value() const override { return it_->value(); }
  [[nodiscard]] ValueType type() const override { return it_->type(); }
  [[nodiscard]] Status status() const override { return it_->status(); }

 private:
  std::shared_ptr<const void> owner_;
  std::unique_ptr<Iterator> it_;
};

}  // namespace

struct DB::Writer {
  WriteBatch* batch = nullptr;
  bool force_flush = false;
  bool done = false;
  Status status;
  std::condition_variable cv;
};

DB::DB(const Options& options, std::string dir)
    : options_(options),
      dir_(std::move(dir)),
      mem_(std::make_shared<MemTable>()),
      version_(std::make_shared<Version>()) {}

Status DB::Open(const Options& options, const std::string& dir, std::unique_ptr<DB>* out) {
  KV_RETURN_IF_ERROR(CreateDirIfMissing(dir));
  std::unique_ptr<DB> db(new DB(options, dir));

  // Two processes appending to one WAL would interleave records; refuse the second.
  const std::string lock_path = JoinPath(dir, "LOCK");
  db->lock_fd_ = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (db->lock_fd_ < 0) {
    return Status::IOErrorFromErrno("open " + lock_path);
  }
  if (::flock(db->lock_fd_, LOCK_EX | LOCK_NB) != 0) {
    return Status::IOError(dir + " is locked by another process");
  }

  KV_RETURN_IF_ERROR(db->Recover());
  db->bg_thread_ = std::thread(&DB::BackgroundLoop, db.get());
  *out = std::move(db);
  return Status::OK();
}

DB::~DB() {
  {
    std::lock_guard lk(mu_);
    shutting_down_ = true;
  }
  bg_cv_.notify_all();
  if (bg_thread_.joinable()) {
    bg_thread_.join();
  }
  if (wal_) {
    (void)wal_->Close();
  }
  if (lock_fd_ >= 0) {
    ::flock(lock_fd_, LOCK_UN);
    ::close(lock_fd_);
  }
}

// ---------------------------------------------------------------------------------------
// Recovery

Status DB::Recover() {
  ManifestState state;
  Status s = ReadManifest(dir_, &state);
  if (s.IsNotFound()) {
    state = ManifestState{};
  } else if (!s.ok()) {
    return s;
  }

  auto version = std::make_shared<Version>();
  for (const uint64_t number : state.tables) {
    std::shared_ptr<Table> table;
    KV_RETURN_IF_ERROR(Table::Open(TableFileName(dir_, number), number, &table));
    version->tables.push_back(std::move(table));
  }

  std::vector<std::string> names;
  KV_RETURN_IF_ERROR(ListDir(dir_, &names));
  std::vector<uint64_t> logs;
  next_file_number_ = state.next_file_number;
  for (const auto& name : names) {
    uint64_t number = 0;
    const FileType type = ParseFileName(name, &number);
    if (type == FileType::kOther) {
      continue;
    }
    next_file_number_ = std::max(next_file_number_, number + 1);
    if (type == FileType::kLog && number >= state.log_number) {
      logs.push_back(number);
    }
  }
  std::sort(logs.begin(), logs.end());

  auto recovered = std::make_shared<MemTable>();
  uint64_t max_seq = 0;
  for (const uint64_t number : logs) {
    KV_RETURN_IF_ERROR(ReplayLog(number, recovered.get(), &max_seq));
  }
  last_seq_ = max_seq;

  // Write recovered entries straight to an SSTable so the old logs can be deleted and
  // the new WAL starts empty.
  if (!recovered->Empty()) {
    const uint64_t number = next_file_number_++;
    std::shared_ptr<Table> table;
    auto iter = recovered->NewIterator();
    KV_RETURN_IF_ERROR(WriteTable(iter.get(), number, &table));
    version->tables.insert(version->tables.begin(), std::move(table));
    ++stats_.flushes;
  }

  KV_RETURN_IF_ERROR(NewLogFile(&wal_, &wal_number_));
  version_ = version;
  KV_RETURN_IF_ERROR(InstallManifest(*version_, wal_number_));
  RemoveObsoleteFiles();
  return Status::OK();
}

Status DB::ReplayLog(uint64_t number, MemTable* mem, uint64_t* max_seq) {
  std::string contents;
  KV_RETURN_IF_ERROR(ReadFileToString(LogFileName(dir_, number), &contents));
  WalReader reader(contents);
  std::string_view record;
  WriteBatch batch;
  while (reader.Next(&record)) {
    KV_RETURN_IF_ERROR(batch.SetContents(record));
    // A record that passed its CRC but does not decode is real corruption, not a torn
    // tail, so it fails recovery rather than being skipped.
    KV_RETURN_IF_ERROR(InsertInto(batch, mem));
    if (batch.Count() > 0) {
      *max_seq = std::max(*max_seq, batch.Sequence() + batch.Count() - 1);
    }
  }
  if (reader.tail_dropped() > 0) {
    std::fprintf(stderr, "kvstore: dropped %zu bytes of torn tail from %06llu.log\n",
                 reader.tail_dropped(), static_cast<unsigned long long>(number));
  }
  return Status::OK();
}

Status DB::NewLogFile(std::unique_ptr<WalWriter>* wal, uint64_t* number) {
  *number = next_file_number_++;
  std::unique_ptr<AppendFile> file;
  KV_RETURN_IF_ERROR(AppendFile::Open(LogFileName(dir_, *number), &file));
  // The directory entry must be durable too, or an fsynced write could sit in a file
  // that no longer exists after a crash.
  KV_RETURN_IF_ERROR(SyncDir(dir_));
  *wal = std::make_unique<WalWriter>(std::move(file));
  return Status::OK();
}

Status DB::WriteTable(Iterator* iter, uint64_t number, std::shared_ptr<Table>* table) {
  const std::string path = TableFileName(dir_, number);
  std::unique_ptr<AppendFile> file;
  KV_RETURN_IF_ERROR(AppendFile::Open(path, &file));
  TableBuilder builder(options_.table, file.get());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    builder.Add(iter->key(), iter->type(), iter->value());
  }
  Status s = iter->status();
  if (s.ok()) {
    s = builder.Finish();
  }
  if (s.ok()) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  if (s.ok()) {
    s = Table::Open(path, number, table);
  }
  if (!s.ok()) {
    (void)RemoveFile(path);
  }
  return s;
}

Status DB::InstallManifest(const Version& v, uint64_t log_number) {
  ManifestState state;
  state.next_file_number = next_file_number_;
  state.log_number = log_number;
  for (const auto& t : v.tables) {
    state.tables.push_back(t->number());
  }
  return WriteManifest(dir_, state);
}

void DB::RemoveObsoleteFiles() {
  std::vector<uint64_t> live;
  for (const auto& t : version_->tables) {
    live.push_back(t->number());
  }
  const uint64_t min_log = imm_ ? imm_wal_number_ : wal_number_;

  std::vector<std::string> names;
  if (!ListDir(dir_, &names).ok()) {
    return;
  }
  for (const auto& name : names) {
    uint64_t number = 0;
    const FileType type = ParseFileName(name, &number);
    bool remove = false;
    if (type == FileType::kLog) {
      remove = number < min_log;
    } else if (type == FileType::kTable) {
      // Tables still being written are not in the version yet.
      remove = std::find(live.begin(), live.end(), number) == live.end() &&
               pending_outputs_.count(number) == 0;
    }
    if (remove) {
      // Open readers keep their mmap, so unlinking a table under them is safe.
      (void)RemoveFile(JoinPath(dir_, name));
    }
  }
}

// ---------------------------------------------------------------------------------------
// Writes

Status DB::Put(std::string_view key, std::string_view value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(&batch);
}

Status DB::Delete(std::string_view key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(&batch);
}

Status DB::Write(WriteBatch* batch) { return WriteInternal(batch, false); }

Status DB::Flush() {
  KV_RETURN_IF_ERROR(WriteInternal(nullptr, true));
  std::unique_lock lk(mu_);
  done_cv_.wait(lk, [&] { return !imm_ || !bg_error_.ok(); });
  return bg_error_;
}

// WriteInternal implements group commit. Writers queue up; the one at the front logs its
// batch together with those of the writers behind it in one WAL append and one fsync,
// inserts them into the memtable, then wakes them with the shared result. Under load
// this turns N fsyncs into ~1, which is where most write throughput comes from.
Status DB::WriteInternal(WriteBatch* batch, bool force_flush) {
  Writer w;
  w.batch = batch;
  w.force_flush = force_flush;

  std::unique_lock lk(mu_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.wait(lk);
  }
  if (w.done) {
    return w.status;
  }

  Status s = MakeRoomForWrite(lk, force_flush);
  Writer* last = &w;
  if (s.ok() && batch != nullptr) {
    WriteBatch* group = BuildBatchGroup(&last);
    group->SetSequence(last_seq_ + 1);
    last_seq_ += group->Count();

    // Only the front writer touches wal_ and mem_, so they cannot change while the lock
    // is released for the slow part.
    WalWriter* wal = wal_.get();
    MemTable* mem = mem_.get();
    lk.unlock();
    s = wal->AddRecord(group->Contents());
    const bool sync = s.ok() && options_.sync;
    if (sync) {
      s = wal->Sync();
    }
    if (s.ok()) {
      s = InsertInto(*group, mem);
    }
    lk.lock();

    if (sync) {
      ++stats_.wal_syncs;
    }
    stats_.writes += group->Count();
    if (!s.ok()) {
      // The log may now hold a partial record; refuse further writes rather than risk
      // acknowledging one that recovery would drop.
      bg_error_ = s;
    }
    if (group == &group_batch_) {
      group_batch_.Clear();
    }
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = s;
      ready->done = true;
      ready->cv.notify_one();
    }
    if (ready == last) {
      break;
    }
  }
  if (!writers_.empty()) {
    writers_.front()->cv.notify_one();
  }
  return s;
}

WriteBatch* DB::BuildBatchGroup(Writer** last) {
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  *last = first;
  if (!options_.group_commit) {
    return result;
  }
  size_t size = result->ByteSize();
  for (auto it = writers_.begin() + 1; it != writers_.end(); ++it) {
    Writer* w = *it;
    if (w->batch == nullptr || size + w->batch->ByteSize() > options_.max_group_bytes) {
      break;  // Flush requests and oversized groups wait for the next round.
    }
    if (result == first->batch) {
      group_batch_.Clear();
      group_batch_.Append(*first->batch);
      result = &group_batch_;
    }
    result->Append(*w->batch);
    size += w->batch->ByteSize();
    *last = w;
  }
  return result;
}

Status DB::MakeRoomForWrite(std::unique_lock<std::mutex>& lk, bool force) {
  while (true) {
    if (!bg_error_.ok()) {
      return bg_error_;
    }
    if (!force && mem_->ApproximateMemoryUsage() < options_.write_buffer_size) {
      return Status::OK();
    }
    if (imm_) {
      // The previous memtable is still flushing. Stall rather than let memory grow
      // without bound; this is the engine's backpressure.
      done_cv_.wait(lk);
      continue;
    }
    if (mem_->Empty()) {
      return Status::OK();
    }
    std::unique_ptr<WalWriter> wal;
    uint64_t number = 0;
    KV_RETURN_IF_ERROR(NewLogFile(&wal, &number));
    (void)wal_->Close();
    imm_wal_number_ = wal_number_;
    imm_ = std::move(mem_);
    mem_ = std::make_shared<MemTable>();
    wal_ = std::move(wal);
    wal_number_ = number;
    bg_cv_.notify_one();
    force = false;
  }
}

// ---------------------------------------------------------------------------------------
// Background work

void DB::BackgroundLoop() {
  std::unique_lock lk(mu_);
  while (true) {
    bg_cv_.wait(lk, [&] { return shutting_down_ || (imm_ && bg_error_.ok()); });
    if (imm_ && bg_error_.ok()) {
      FlushImmutable(lk);
      continue;
    }
    if (shutting_down_) {
      return;
    }
  }
}

void DB::FlushImmutable(std::unique_lock<std::mutex>& lk) {
  const std::shared_ptr<MemTable> imm = imm_;
  const uint64_t number = next_file_number_++;
  pending_outputs_.insert(number);

  lk.unlock();
  std::shared_ptr<Table> table;
  auto iter = imm->NewIterator();
  Status s = WriteTable(iter.get(), number, &table);
  lk.lock();

  pending_outputs_.erase(number);
  if (s.ok()) {
    auto v = std::make_shared<Version>(*version_);
    v->tables.insert(v->tables.begin(), table);
    // Every write in the immutable memtable's WAL is now in the table, so the oldest
    // log recovery needs is the current one.
    s = InstallManifest(*v, wal_number_);
    if (s.ok()) {
      version_ = std::move(v);
      imm_.reset();
      ++stats_.flushes;
      RemoveObsoleteFiles();
    }
  }
  if (!s.ok()) {
    bg_error_ = s;
  }
  done_cv_.notify_all();
}

// ---------------------------------------------------------------------------------------
// Reads

Status DB::Get(std::string_view key, std::string* value) {
  std::shared_ptr<MemTable> mem;
  std::shared_ptr<MemTable> imm;
  std::shared_ptr<const Version> version;
  {
    std::lock_guard lk(mu_);
    mem = mem_;
    imm = imm_;
    version = version_;
  }

  ValueType type = ValueType::kValue;
  if (mem->Get(key, value, &type) || (imm && imm->Get(key, value, &type))) {
    return type == ValueType::kValue ? Status::OK() : Status::NotFound();
  }
  for (const auto& table : version->tables) {
    Status s = table->Get(key, value, &type);
    if (s.ok()) {
      return type == ValueType::kValue ? Status::OK() : Status::NotFound();
    }
    if (!s.IsNotFound()) {
      return s;
    }
  }
  return Status::NotFound();
}

std::unique_ptr<Iterator> DB::NewIterator() {
  std::shared_ptr<MemTable> mem;
  std::shared_ptr<MemTable> imm;
  std::shared_ptr<const Version> version;
  {
    std::lock_guard lk(mu_);
    mem = mem_;
    imm = imm_;
    version = version_;
  }
  std::vector<std::unique_ptr<Iterator>> children;
  children.push_back(std::make_unique<Pinned>(mem, mem->NewIterator()));
  if (imm) {
    children.push_back(std::make_unique<Pinned>(imm, imm->NewIterator()));
  }
  for (const auto& table : version->tables) {
    children.push_back(table->NewIterator());
  }
  return NewLiveIterator(NewMergingIterator(std::move(children)));
}

DBStats DB::GetStats() {
  std::lock_guard lk(mu_);
  DBStats s = stats_;
  s.num_tables = version_->tables.size();
  s.table_bytes = 0;
  for (const auto& t : version_->tables) {
    s.table_bytes += t->file_size();
  }
  s.memtable_bytes = mem_->ApproximateMemoryUsage();
  return s;
}

}  // namespace kv::storage

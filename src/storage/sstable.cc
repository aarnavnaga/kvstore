#include "storage/sstable.h"

#include <algorithm>
#include <cassert>
#include <cstdio>

#include "common/coding.h"
#include "common/crc32c.h"

namespace kv::storage {
namespace {

constexpr uint64_t kMagic = 0x4b5653535441424cULL;  // "KVSSTABL"
constexpr size_t kFooterSize = 56;                  // seven fixed64 fields
constexpr size_t kCrcSize = 4;

// CheckedBlock validates [offset, offset+size) plus its trailing CRC against contents.
Status CheckedBlock(std::string_view contents, uint64_t offset, uint64_t size,
                    std::string_view* out) {
  if (offset > contents.size() || size > contents.size() - offset ||
      kCrcSize > contents.size() - offset - size) {
    return Status::Corruption("sstable block out of range");
  }
  const std::string_view block = contents.substr(offset, size);
  const uint32_t want = DecodeFixed32(contents.data() + offset + size);
  if (crc32c::Value(block) != want) {
    return Status::Corruption("sstable block checksum mismatch");
  }
  *out = block;
  return Status::OK();
}

}  // namespace

std::string TableFileName(const std::string& dir, uint64_t number) {
  char name[32];
  std::snprintf(name, sizeof(name), "%06llu.sst", static_cast<unsigned long long>(number));
  return JoinPath(dir, name);
}

bool DecodeBlockEntry(std::string_view* in, BlockEntry* entry) {
  uint8_t type = 0;
  uint64_t klen = 0;
  uint64_t vlen = 0;
  if (!GetByte(in, &type) || !GetVarint64(in, &klen) || !GetVarint64(in, &vlen)) {
    return false;
  }
  if (type > static_cast<uint8_t>(ValueType::kValue) || klen > in->size() ||
      vlen > in->size() - klen) {
    return false;
  }
  entry->type = static_cast<ValueType>(type);
  entry->key = in->substr(0, klen);
  entry->value = in->substr(klen, vlen);
  in->remove_prefix(klen + vlen);
  return true;
}

// ---------------------------------------------------------------------------------------
// TableBuilder

TableBuilder::TableBuilder(const TableOptions& options, AppendFile* file)
    : options_(options), file_(file), bloom_(options.bloom_bits_per_key) {}

void TableBuilder::Add(std::string_view key, ValueType type, std::string_view value) {
  assert(num_entries_ == 0 || key > last_key_);
  block_.push_back(static_cast<char>(type));
  PutVarint64(&block_, key.size());
  PutVarint64(&block_, value.size());
  block_.append(key);
  block_.append(value);
  bloom_.AddKey(key);
  last_key_.assign(key);
  ++num_entries_;
  if (type == ValueType::kDeletion) {
    ++num_tombstones_;
  }
  if (block_.size() >= options_.block_size) {
    FlushBlock();
  }
}

void TableBuilder::WriteBlock(std::string_view contents) {
  if (!status_.ok()) {
    return;
  }
  std::string buf(contents);
  PutFixed32(&buf, crc32c::Value(contents));
  status_ = file_->Append(buf);
  offset_ += buf.size();
}

void TableBuilder::FlushBlock() {
  if (block_.empty()) {
    return;
  }
  PutLengthPrefixed(&index_, last_key_);
  PutVarint64(&index_, offset_);
  PutVarint64(&index_, block_.size());
  WriteBlock(block_);
  block_.clear();
}

Status TableBuilder::Finish() {
  FlushBlock();

  const uint64_t bloom_offset = offset_;
  const std::string bloom = bloom_.Finish();
  WriteBlock(bloom);

  const uint64_t index_offset = offset_;
  WriteBlock(index_);

  std::string footer;
  PutFixed64(&footer, index_offset);
  PutFixed64(&footer, index_.size());
  PutFixed64(&footer, bloom_offset);
  PutFixed64(&footer, bloom.size());
  PutFixed64(&footer, num_entries_);
  PutFixed64(&footer, num_tombstones_);
  PutFixed64(&footer, kMagic);
  if (status_.ok()) {
    status_ = file_->Append(footer);
    offset_ += footer.size();
  }
  return status_;
}

// ---------------------------------------------------------------------------------------
// Table

Status Table::Open(const std::string& path, uint64_t number, std::shared_ptr<Table>* out) {
  std::unique_ptr<MappedFile> file;
  KV_RETURN_IF_ERROR(MappedFile::Open(path, &file));
  std::shared_ptr<Table> table(new Table());
  table->contents_ = file->data();
  table->file_ = std::move(file);
  table->number_ = number;
  Status s = table->Parse();
  if (!s.ok()) {
    return Status::Corruption(path + ": " + s.message());
  }
  *out = std::move(table);
  return Status::OK();
}

Status Table::OpenFromContents(std::string_view contents, uint64_t number,
                               std::shared_ptr<Table>* out) {
  std::shared_ptr<Table> table(new Table());
  table->contents_ = contents;
  table->number_ = number;
  KV_RETURN_IF_ERROR(table->Parse());
  *out = std::move(table);
  return Status::OK();
}

Status Table::Parse() {
  if (contents_.size() < kFooterSize) {
    return Status::Corruption("sstable shorter than footer");
  }
  std::string_view footer = contents_.substr(contents_.size() - kFooterSize);
  uint64_t index_offset = 0;
  uint64_t index_size = 0;
  uint64_t bloom_offset = 0;
  uint64_t bloom_size = 0;
  uint64_t magic = 0;
  GetFixed64(&footer, &index_offset);
  GetFixed64(&footer, &index_size);
  GetFixed64(&footer, &bloom_offset);
  GetFixed64(&footer, &bloom_size);
  GetFixed64(&footer, &num_entries_);
  GetFixed64(&footer, &num_tombstones_);
  GetFixed64(&footer, &magic);
  if (magic != kMagic) {
    return Status::Corruption("bad sstable magic");
  }
  const std::string_view body = contents_.substr(0, contents_.size() - kFooterSize);

  KV_RETURN_IF_ERROR(CheckedBlock(body, bloom_offset, bloom_size, &bloom_));
  std::string_view index;
  KV_RETURN_IF_ERROR(CheckedBlock(body, index_offset, index_size, &index));

  while (!index.empty()) {
    IndexEntry e;
    std::string_view last_key;
    if (!GetLengthPrefixed(&index, &last_key) || !GetVarint64(&index, &e.offset) ||
        !GetVarint64(&index, &e.size)) {
      return Status::Corruption("malformed sstable index");
    }
    if (e.offset >= bloom_offset || e.size > bloom_offset - e.offset) {
      return Status::Corruption("sstable index entry points outside data region");
    }
    if (!index_.empty() && last_key <= index_.back().last_key) {
      return Status::Corruption("sstable index keys out of order");
    }
    e.last_key.assign(last_key);
    index_.push_back(std::move(e));
  }

  if (!index_.empty()) {
    std::string_view first;
    KV_RETURN_IF_ERROR(ReadBlock(0, &first));
    BlockEntry entry;
    if (!DecodeBlockEntry(&first, &entry)) {
      return Status::Corruption("malformed first sstable entry");
    }
    smallest_.assign(entry.key);
    largest_ = index_.back().last_key;
  }
  return Status::OK();
}

Status Table::ReadBlock(size_t i, std::string_view* block) const {
  return CheckedBlock(contents_, index_[i].offset, index_[i].size, block);
}

Status Table::Get(std::string_view key, std::string* value, ValueType* type) const {
  if (index_.empty() || key < smallest_ || key > largest_ || !BloomMayContain(bloom_, key)) {
    return Status::NotFound();
  }
  // The first block whose last key is >= key is the only one that can hold it.
  const auto it = std::lower_bound(
      index_.begin(), index_.end(), key,
      [](const IndexEntry& e, std::string_view k) { return std::string_view(e.last_key) < k; });
  if (it == index_.end()) {
    return Status::NotFound();
  }
  std::string_view block;
  KV_RETURN_IF_ERROR(ReadBlock(static_cast<size_t>(it - index_.begin()), &block));
  BlockEntry entry;
  while (!block.empty()) {
    if (!DecodeBlockEntry(&block, &entry)) {
      return Status::Corruption("malformed sstable entry");
    }
    if (entry.key == key) {
      *type = entry.type;
      if (entry.type == ValueType::kValue) {
        value->assign(entry.value);
      }
      return Status::OK();
    }
    if (entry.key > key) {
      break;
    }
  }
  return Status::NotFound();
}

class Table::Iter final : public Iterator {
 public:
  explicit Iter(std::shared_ptr<const Table> table) : table_(std::move(table)) {}

  [[nodiscard]] bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    LoadBlock(0);
    Advance();
  }

  void Seek(std::string_view target) override {
    const auto& index = table_->index_;
    const auto it = std::lower_bound(
        index.begin(), index.end(), target,
        [](const IndexEntry& e, std::string_view k) { return std::string_view(e.last_key) < k; });
    LoadBlock(static_cast<size_t>(it - index.begin()));
    Advance();
    while (valid_ && entry_.key < target) {
      Advance();
    }
  }

  void Next() override { Advance(); }

  [[nodiscard]] std::string_view key() const override { return entry_.key; }
  [[nodiscard]] std::string_view value() const override { return entry_.value; }
  [[nodiscard]] ValueType type() const override { return entry_.type; }
  [[nodiscard]] Status status() const override { return status_; }

 private:
  void LoadBlock(size_t i) {
    block_index_ = i;
    rest_ = {};
    if (i < table_->index_.size()) {
      status_ = table_->ReadBlock(i, &rest_);
    }
  }

  // Advance moves to the next entry, crossing into later blocks as needed.
  void Advance() {
    while (status_.ok()) {
      if (!rest_.empty()) {
        if (DecodeBlockEntry(&rest_, &entry_)) {
          valid_ = true;
          return;
        }
        status_ = Status::Corruption("malformed sstable entry");
        break;
      }
      if (block_index_ + 1 >= table_->index_.size()) {
        break;
      }
      LoadBlock(block_index_ + 1);
    }
    valid_ = false;
  }

  std::shared_ptr<const Table> table_;
  size_t block_index_ = 0;
  std::string_view rest_;
  BlockEntry entry_;
  bool valid_ = false;
  Status status_;
};

std::unique_ptr<Iterator> Table::NewIterator() const {
  return std::make_unique<Iter>(shared_from_this());
}

}  // namespace kv::storage

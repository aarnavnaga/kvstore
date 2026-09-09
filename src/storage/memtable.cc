#include "storage/memtable.h"

#include <cstring>

#include "common/coding.h"

namespace kv::storage {
namespace {

// Entry layout in the arena:
//   varint32 key_len | key | fixed64 (seq << 8 | type) | varint32 value_len | value
struct Decoded {
  std::string_view key;
  uint64_t tag;
  std::string_view value;
};

Decoded Decode(const char* entry) {
  // Entries are written by Add, so the encoding is trusted; the limits only bound the
  // varint scan.
  std::string_view in(entry, 5);
  uint32_t klen = 0;
  GetVarint32(&in, &klen);
  const char* p = in.data();
  Decoded d;
  d.key = std::string_view(p, klen);
  p += klen;
  d.tag = DecodeFixed64(p);
  p += 8;
  std::string_view vin(p, 5);
  uint32_t vlen = 0;
  GetVarint32(&vin, &vlen);
  d.value = std::string_view(vin.data(), vlen);
  return d;
}

constexpr uint64_t kMaxSequence = (uint64_t{1} << 56) - 1;

}  // namespace

int MemTable::KeyComparator::operator()(const char* a, const char* b) const {
  const Decoded da = Decode(a);
  const Decoded db = Decode(b);
  const int c = da.key.compare(db.key);
  if (c != 0) {
    return c;
  }
  // Higher tag (newer sequence) sorts first.
  if (da.tag > db.tag) {
    return -1;
  }
  return da.tag < db.tag ? 1 : 0;
}

MemTable::MemTable() : table_(KeyComparator{}, &arena_) {}

void MemTable::Add(uint64_t seq, ValueType type, std::string_view key, std::string_view value) {
  std::string header;
  PutVarint32(&header, static_cast<uint32_t>(key.size()));
  std::string vheader;
  PutVarint32(&vheader, static_cast<uint32_t>(value.size()));

  const size_t len = header.size() + key.size() + 8 + vheader.size() + value.size();
  char* buf = arena_.Allocate(len);
  char* p = buf;
  std::memcpy(p, header.data(), header.size());
  p += header.size();
  // string_view's data() may be null when empty, and memcpy(dst, nullptr, 0) is UB.
  if (!key.empty()) {
    std::memcpy(p, key.data(), key.size());
  }
  p += key.size();
  EncodeFixed64(p, (seq << 8) | static_cast<uint64_t>(type));
  p += 8;
  std::memcpy(p, vheader.data(), vheader.size());
  p += vheader.size();
  if (!value.empty()) {
    std::memcpy(p, value.data(), value.size());
  }

  table_.Insert(buf);
  num_entries_.fetch_add(1, std::memory_order_relaxed);
}

bool MemTable::Get(std::string_view key, std::string* value, ValueType* type) const {
  // A lookup key with the maximum sequence sorts before every real entry for this key.
  std::string lookup;
  PutVarint32(&lookup, static_cast<uint32_t>(key.size()));
  lookup.append(key);
  PutFixed64(&lookup, (kMaxSequence << 8) | 0xff);
  PutVarint32(&lookup, 0);

  Table::Iterator it(&table_);
  it.Seek(lookup.data());
  if (!it.Valid()) {
    return false;
  }
  const Decoded d = Decode(it.key());
  if (d.key != key) {
    return false;
  }
  *type = static_cast<ValueType>(d.tag & 0xff);
  if (*type == ValueType::kValue) {
    value->assign(d.value);
  }
  return true;
}

class MemTable::Iter final : public Iterator {
 public:
  explicit Iter(const Table* table) : it_(table) {}

  [[nodiscard]] bool Valid() const override { return it_.Valid(); }
  void SeekToFirst() override {
    it_.SeekToFirst();
    Load();
  }
  void Seek(std::string_view target) override {
    std::string lookup;
    PutVarint32(&lookup, static_cast<uint32_t>(target.size()));
    lookup.append(target);
    PutFixed64(&lookup, (kMaxSequence << 8) | 0xff);
    PutVarint32(&lookup, 0);
    it_.Seek(lookup.data());
    Load();
  }
  void Next() override {
    // Skip the older versions of the key we are leaving.
    const std::string prev(cur_.key);
    it_.Next();
    while (it_.Valid() && Decode(it_.key()).key == prev) {
      it_.Next();
    }
    Load();
  }
  [[nodiscard]] std::string_view key() const override { return cur_.key; }
  [[nodiscard]] std::string_view value() const override { return cur_.value; }
  [[nodiscard]] ValueType type() const override { return static_cast<ValueType>(cur_.tag & 0xff); }

 private:
  void Load() {
    if (it_.Valid()) {
      cur_ = Decode(it_.key());
    }
  }

  Table::Iterator it_;
  Decoded cur_{};
};

std::unique_ptr<Iterator> MemTable::NewIterator() const { return std::make_unique<Iter>(&table_); }

}  // namespace kv::storage

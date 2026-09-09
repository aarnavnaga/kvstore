#include "storage/write_batch.h"

#include "common/coding.h"

namespace kv::storage {

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader, '\0');
}

void WriteBatch::Put(std::string_view key, std::string_view value) {
  EncodeFixed32(rep_.data() + 8, Count() + 1);
  rep_.push_back(static_cast<char>(ValueType::kValue));
  PutLengthPrefixed(&rep_, key);
  PutLengthPrefixed(&rep_, value);
}

void WriteBatch::Delete(std::string_view key) {
  EncodeFixed32(rep_.data() + 8, Count() + 1);
  rep_.push_back(static_cast<char>(ValueType::kDeletion));
  PutLengthPrefixed(&rep_, key);
}

void WriteBatch::Append(const WriteBatch& other) {
  EncodeFixed32(rep_.data() + 8, Count() + other.Count());
  rep_.append(other.rep_, kHeader, std::string::npos);
}

uint32_t WriteBatch::Count() const { return DecodeFixed32(rep_.data() + 8); }
uint64_t WriteBatch::Sequence() const { return DecodeFixed64(rep_.data()); }
void WriteBatch::SetSequence(uint64_t seq) { EncodeFixed64(rep_.data(), seq); }

Status WriteBatch::SetContents(std::string_view contents) {
  if (contents.size() < kHeader) {
    return Status::Corruption("write batch shorter than header");
  }
  rep_.assign(contents);
  return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
  std::string_view in(rep_);
  in.remove_prefix(kHeader);
  uint32_t found = 0;
  while (!in.empty()) {
    uint8_t tag = 0;
    std::string_view key;
    std::string_view value;
    if (!GetByte(&in, &tag) || !GetLengthPrefixed(&in, &key)) {
      return Status::Corruption("truncated write batch entry");
    }
    switch (static_cast<ValueType>(tag)) {
      case ValueType::kValue:
        if (!GetLengthPrefixed(&in, &value)) {
          return Status::Corruption("truncated write batch value");
        }
        handler->Put(key, value);
        break;
      case ValueType::kDeletion:
        handler->Delete(key);
        break;
      default:
        return Status::Corruption("unknown write batch tag");
    }
    ++found;
  }
  if (found != Count()) {
    return Status::Corruption("write batch count mismatch");
  }
  return Status::OK();
}

}  // namespace kv::storage

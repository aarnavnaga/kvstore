#include "storage/wal.h"

#include "common/coding.h"
#include "common/crc32c.h"

namespace kv::storage {
namespace {

constexpr size_t kHeaderSize = 8;
// Bounds a single record so a corrupt length cannot make the reader skip the whole file.
constexpr uint32_t kMaxRecordSize = 256u << 20;

uint32_t RecordCrc(const char* length_field, std::string_view payload) {
  const uint32_t crc = crc32c::Value(length_field, 4);
  return crc32c::Extend(crc, payload.data(), payload.size());
}

}  // namespace

std::string EncodeRecord(std::string_view payload) {
  std::string out;
  out.resize(kHeaderSize);
  EncodeFixed32(out.data() + 4, static_cast<uint32_t>(payload.size()));
  EncodeFixed32(out.data(), RecordCrc(out.data() + 4, payload));
  out.append(payload);
  return out;
}

Status WalWriter::AddRecord(std::string_view payload) {
  if (payload.size() > kMaxRecordSize) {
    return Status::InvalidArgument("WAL record too large");
  }
  // One write() per record: the header and payload land together or the CRC catches it.
  return file_->Append(EncodeRecord(payload));
}

bool WalReader::Next(std::string_view* record) {
  if (rest_.size() < kHeaderSize) {
    dropped_ += rest_.size();
    rest_ = {};
    return false;
  }
  const uint32_t crc = DecodeFixed32(rest_.data());
  const uint32_t len = DecodeFixed32(rest_.data() + 4);
  if (len > kMaxRecordSize || rest_.size() - kHeaderSize < len) {
    dropped_ += rest_.size();
    rest_ = {};
    return false;
  }
  const std::string_view payload = rest_.substr(kHeaderSize, len);
  if (RecordCrc(rest_.data() + 4, payload) != crc) {
    dropped_ += rest_.size();
    rest_ = {};
    return false;
  }
  *record = payload;
  rest_.remove_prefix(kHeaderSize + len);
  return true;
}

}  // namespace kv::storage

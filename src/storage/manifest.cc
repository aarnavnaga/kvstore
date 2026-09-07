#include "storage/manifest.h"

#include "common/coding.h"
#include "common/crc32c.h"
#include "common/file.h"

namespace kv::storage {
namespace {

constexpr uint32_t kMagic = 0x464d564b;  // "KVMF"
constexpr uint32_t kVersion = 1;
constexpr char kFileName[] = "MANIFEST";

}  // namespace

std::string EncodeManifest(const ManifestState& state) {
  std::string out;
  PutFixed32(&out, kMagic);
  PutFixed32(&out, kVersion);
  PutFixed64(&out, state.next_file_number);
  PutFixed64(&out, state.log_number);
  PutFixed32(&out, static_cast<uint32_t>(state.tables.size()));
  for (const uint64_t t : state.tables) {
    PutFixed64(&out, t);
  }
  PutFixed32(&out, crc32c::Value(out));
  return out;
}

Status DecodeManifest(std::string_view data, ManifestState* state) {
  if (data.size() < 4) {
    return Status::Corruption("manifest too short");
  }
  const std::string_view body = data.substr(0, data.size() - 4);
  if (crc32c::Value(body) != DecodeFixed32(data.data() + body.size())) {
    return Status::Corruption("manifest checksum mismatch");
  }
  std::string_view in = body;
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t count = 0;
  if (!GetFixed32(&in, &magic) || magic != kMagic || !GetFixed32(&in, &version) ||
      version != kVersion || !GetFixed64(&in, &state->next_file_number) ||
      !GetFixed64(&in, &state->log_number) || !GetFixed32(&in, &count) ||
      in.size() != static_cast<size_t>(count) * 8) {
    return Status::Corruption("malformed manifest");
  }
  state->tables.resize(count);
  for (uint32_t i = 0; i < count; ++i) {
    GetFixed64(&in, &state->tables[i]);
  }
  return Status::OK();
}

Status WriteManifest(const std::string& dir, const ManifestState& state) {
  return WriteFileAtomic(JoinPath(dir, kFileName), EncodeManifest(state));
}

Status ReadManifest(const std::string& dir, ManifestState* state) {
  std::string data;
  KV_RETURN_IF_ERROR(ReadFileToString(JoinPath(dir, kFileName), &data));
  return DecodeManifest(data, state);
}

}  // namespace kv::storage

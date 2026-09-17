// Treats the input as a WAL file and replays every intact record as a write batch, as
// recovery does after a crash. Recovery reads whatever a crash left on disk.

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "storage/memtable.h"
#include "storage/wal.h"
#include "storage/write_batch.h"

namespace {

class Sink final : public kv::storage::WriteBatch::Handler {
 public:
  void Put(std::string_view key, std::string_view value) override {
    mem.Add(seq++, kv::storage::ValueType::kValue, key, value);
  }
  void Delete(std::string_view key) override {
    mem.Add(seq++, kv::storage::ValueType::kDeletion, key, {});
  }
  kv::storage::MemTable mem;
  uint64_t seq = 1;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  kv::storage::WalReader reader(std::string_view(reinterpret_cast<const char*>(data), size));
  std::string_view record;
  Sink sink;
  while (reader.Next(&record)) {
    kv::storage::WriteBatch batch;
    if (batch.SetContents(record).ok()) {
      (void)batch.Iterate(&sink);
    }
  }
  return 0;
}

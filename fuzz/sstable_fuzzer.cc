// Parses the input as an SSTable image, then iterates it and looks up the keys it finds.
// Exercises footer, index, bloom filter and data-block decoding on corrupt files.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/sstable.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::string contents(reinterpret_cast<const char*>(data), size);
  std::shared_ptr<kv::storage::Table> table;
  if (!kv::storage::Table::OpenFromContents(contents, 1, &table).ok()) {
    return 0;
  }
  std::vector<std::string> keys;
  auto it = table->NewIterator();
  for (it->SeekToFirst(); it->Valid() && keys.size() < 64; it->Next()) {
    keys.emplace_back(it->key());
  }
  std::string value;
  kv::storage::ValueType type{};
  for (const auto& k : keys) {
    (void)table->Get(k, &value, &type);
    it->Seek(k);
  }
  (void)table->Get("absent", &value, &type);
  return 0;
}

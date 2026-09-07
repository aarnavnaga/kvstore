#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv::storage {

// BloomBuilder collects keys for one SSTable and produces its filter block. At 10 bits per
// key with 7 probes the false-positive rate is about 1%, so a GET for an absent key skips
// ~99% of the tables it would otherwise have to search.
class BloomBuilder {
 public:
  explicit BloomBuilder(int bits_per_key);

  void AddKey(std::string_view key);
  // Finish encodes the filter as: bit array | u8 probe count.
  [[nodiscard]] std::string Finish() const;

 private:
  int bits_per_key_;
  int probes_;
  std::vector<uint64_t> hashes_;
};

// BloomMayContain reports whether key may be in the set encoded by filter. A malformed or
// empty filter answers true so it can only cost a read, never hide a key.
bool BloomMayContain(std::string_view filter, std::string_view key);

uint64_t BloomHash(std::string_view key);

}  // namespace kv::storage

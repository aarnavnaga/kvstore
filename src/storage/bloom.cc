#include "storage/bloom.h"

#include <algorithm>
#include <cmath>

namespace kv::storage {

uint64_t BloomHash(std::string_view key) {
  // FNV-1a followed by a murmur3 finalizer: cheap, and the finalizer spreads entropy
  // into the high bits that the second probe hash uses.
  uint64_t h = 0xcbf29ce484222325ULL;
  for (const char c : key) {
    h ^= static_cast<unsigned char>(c);
    h *= 0x100000001b3ULL;
  }
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  return h;
}

BloomBuilder::BloomBuilder(int bits_per_key)
    : bits_per_key_(std::max(1, bits_per_key)),
      // k = bits_per_key * ln 2 minimizes the false-positive rate.
      probes_(std::clamp(static_cast<int>(bits_per_key_ * 0.69), 1, 30)) {}

void BloomBuilder::AddKey(std::string_view key) { hashes_.push_back(BloomHash(key)); }

std::string BloomBuilder::Finish() const {
  size_t bits = std::max<size_t>(64, hashes_.size() * static_cast<size_t>(bits_per_key_));
  const size_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  std::string filter(bytes, '\0');
  for (const uint64_t h : hashes_) {
    // Double hashing (Kirsch-Mitzenmacher): probe i is h1 + i*h2.
    const auto h1 = static_cast<uint32_t>(h);
    const auto h2 = static_cast<uint32_t>(h >> 32) | 1;
    for (int i = 0; i < probes_; ++i) {
      const size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % bits;
      filter[bit / 8] = static_cast<char>(filter[bit / 8] | (1 << (bit % 8)));
    }
  }
  filter.push_back(static_cast<char>(probes_));
  return filter;
}

bool BloomMayContain(std::string_view filter, std::string_view key) {
  if (filter.size() < 2) {
    return true;
  }
  const int probes = static_cast<unsigned char>(filter.back());
  if (probes < 1 || probes > 30) {
    return true;
  }
  const size_t bits = (filter.size() - 1) * 8;
  const uint64_t h = BloomHash(key);
  const auto h1 = static_cast<uint32_t>(h);
  const auto h2 = static_cast<uint32_t>(h >> 32) | 1;
  for (int i = 0; i < probes; ++i) {
    const size_t bit = (h1 + static_cast<uint64_t>(i) * h2) % bits;
    if ((static_cast<unsigned char>(filter[bit / 8]) & (1 << (bit % 8))) == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace kv::storage

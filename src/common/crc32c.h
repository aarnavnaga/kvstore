#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace kv::crc32c {

// Extend returns the CRC32C (Castagnoli) of data appended to a stream whose CRC so far is
// init. CRC32C is the variant used by iSCSI, ext4 and LevelDB; it detects all burst errors
// up to 32 bits, which covers a torn sector write at the tail of the WAL.
uint32_t Extend(uint32_t init, const char* data, size_t n);

inline uint32_t Value(const char* data, size_t n) { return Extend(0, data, n); }
inline uint32_t Value(std::string_view s) { return Extend(0, s.data(), s.size()); }

}  // namespace kv::crc32c

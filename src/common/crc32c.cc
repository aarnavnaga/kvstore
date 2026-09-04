#include "common/crc32c.h"

#include <array>

namespace kv::crc32c {
namespace {

constexpr uint32_t kPoly = 0x82F63B78;  // Castagnoli polynomial, bit-reversed.

// Slicing-by-8 tables: table[k][b] is the CRC of byte b followed by k zero bytes, which
// lets the loop below fold eight input bytes per iteration instead of one.
constexpr std::array<std::array<uint32_t, 256>, 8> MakeTables() {
  std::array<std::array<uint32_t, 256>, 8> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t crc = i;
    for (int j = 0; j < 8; ++j) {
      crc = (crc >> 1) ^ ((crc & 1) ? kPoly : 0);
    }
    t[0][i] = crc;
  }
  for (uint32_t i = 0; i < 256; ++i) {
    for (size_t k = 1; k < 8; ++k) {
      t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xff];
    }
  }
  return t;
}

constexpr auto kTables = MakeTables();

}  // namespace

uint32_t Extend(uint32_t init, const char* data, size_t n) {
  const auto* p = reinterpret_cast<const unsigned char*>(data);
  uint32_t crc = ~init;
  while (n >= 8) {
    const uint32_t lo =
        crc ^ (static_cast<uint32_t>(p[0]) | static_cast<uint32_t>(p[1]) << 8 |
               static_cast<uint32_t>(p[2]) << 16 | static_cast<uint32_t>(p[3]) << 24);
    crc = kTables[7][lo & 0xff] ^ kTables[6][(lo >> 8) & 0xff] ^ kTables[5][(lo >> 16) & 0xff] ^
          kTables[4][lo >> 24] ^ kTables[3][p[4]] ^ kTables[2][p[5]] ^ kTables[1][p[6]] ^
          kTables[0][p[7]];
    p += 8;
    n -= 8;
  }
  while (n-- > 0) {
    crc = kTables[0][(crc ^ *p++) & 0xff] ^ (crc >> 8);
  }
  return ~crc;
}

}  // namespace kv::crc32c

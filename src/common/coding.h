#pragma once

// Little-endian fixed-width and varint encodings shared by the WAL, SSTables, the Raft log
// and the wire protocol. Decoders take a string_view by pointer and advance it past what
// they consumed, returning false on truncated input instead of reading out of bounds.

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace kv {

inline void EncodeFixed32(char* dst, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    dst[i] = static_cast<char>(v >> (8 * i));
  }
}

inline void EncodeFixed64(char* dst, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    dst[i] = static_cast<char>(v >> (8 * i));
  }
}

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  }
  return v;
}

inline void PutFixed32(std::string* dst, uint32_t v) {
  char buf[4];
  EncodeFixed32(buf, v);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  char buf[8];
  EncodeFixed64(buf, v);
  dst->append(buf, sizeof(buf));
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  while (v >= 0x80) {
    dst->push_back(static_cast<char>(v | 0x80));
    v >>= 7;
  }
  dst->push_back(static_cast<char>(v));
}

inline void PutVarint32(std::string* dst, uint32_t v) { PutVarint64(dst, v); }

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutVarint64(dst, s.size());
  dst->append(s);
}

inline bool GetFixed32(std::string_view* in, uint32_t* v) {
  if (in->size() < 4) {
    return false;
  }
  *v = DecodeFixed32(in->data());
  in->remove_prefix(4);
  return true;
}

inline bool GetFixed64(std::string_view* in, uint64_t* v) {
  if (in->size() < 8) {
    return false;
  }
  *v = DecodeFixed64(in->data());
  in->remove_prefix(8);
  return true;
}

inline bool GetVarint64(std::string_view* in, uint64_t* v) {
  uint64_t result = 0;
  for (size_t i = 0; i < in->size() && i < 10; ++i) {
    const auto byte = static_cast<unsigned char>((*in)[i]);
    result |= static_cast<uint64_t>(byte & 0x7f) << (7 * i);
    if ((byte & 0x80) == 0) {
      *v = result;
      in->remove_prefix(i + 1);
      return true;
    }
  }
  return false;
}

inline bool GetVarint32(std::string_view* in, uint32_t* v) {
  uint64_t wide = 0;
  if (!GetVarint64(in, &wide) || wide > UINT32_MAX) {
    return false;
  }
  *v = static_cast<uint32_t>(wide);
  return true;
}

inline bool GetLengthPrefixed(std::string_view* in, std::string_view* out) {
  uint64_t len = 0;
  if (!GetVarint64(in, &len) || len > in->size()) {
    return false;
  }
  *out = in->substr(0, len);
  in->remove_prefix(len);
  return true;
}

inline bool GetByte(std::string_view* in, uint8_t* v) {
  if (in->empty()) {
    return false;
  }
  *v = static_cast<uint8_t>((*in)[0]);
  in->remove_prefix(1);
  return true;
}

}  // namespace kv

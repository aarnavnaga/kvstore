#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/iterator.h"

namespace kv::storage {

// WriteBatch is an atomic group of puts and deletes. Its encoding is exactly what the WAL
// stores, so logging a batch is one append:
//
//   fixed64 sequence | fixed32 count | count x (u8 type | varint key | [varint value])
//
// The sequence is assigned by the DB at commit time; entry i gets sequence + i.
class WriteBatch {
 public:
  WriteBatch() { Clear(); }

  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);
  void Clear();
  // Append adds every entry of other to this batch.
  void Append(const WriteBatch& other);

  [[nodiscard]] uint32_t Count() const;
  [[nodiscard]] uint64_t Sequence() const;
  void SetSequence(uint64_t seq);
  [[nodiscard]] size_t ByteSize() const { return rep_.size(); }
  [[nodiscard]] std::string_view Contents() const { return rep_; }

  // SetContents replaces the batch with an encoded one, e.g. a WAL record. It is
  // validated before use by Iterate.
  Status SetContents(std::string_view contents);

  class Handler {
   public:
    virtual ~Handler() = default;
    virtual void Put(std::string_view key, std::string_view value) = 0;
    virtual void Delete(std::string_view key) = 0;
  };
  // Iterate replays the batch into handler, failing with Corruption on a malformed
  // encoding or a count that does not match the entries.
  Status Iterate(Handler* handler) const;

 private:
  static constexpr size_t kHeader = 12;

  std::string rep_;
};

}  // namespace kv::storage

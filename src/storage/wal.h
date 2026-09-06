#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "common/file.h"
#include "common/status.h"

namespace kv::storage {

// Write-ahead log record framing:
//
//   fixed32 crc32c(length | payload) | fixed32 length | payload
//
// A crash can tear the final record (the process died between write() and fsync(), or
// the disk persisted only some sectors). The CRC turns a torn tail into a clean end of
// log instead of garbage replayed into the memtable.
class WalWriter {
 public:
  explicit WalWriter(std::unique_ptr<AppendFile> file) : file_(std::move(file)) {}

  Status AddRecord(std::string_view payload);
  Status Sync() { return file_->Sync(); }
  Status Close() { return file_->Close(); }
  [[nodiscard]] uint64_t size() const { return file_->size(); }

 private:
  std::unique_ptr<AppendFile> file_;
};

// WalReader iterates the records in an in-memory copy of a log file.
class WalReader {
 public:
  explicit WalReader(std::string_view contents) : rest_(contents) {}

  // Next returns the next intact record. It returns false at end of log, including at a
  // torn or corrupt record; tail_dropped() then reports how many bytes were discarded.
  bool Next(std::string_view* record);
  [[nodiscard]] size_t tail_dropped() const { return dropped_; }

 private:
  std::string_view rest_;
  size_t dropped_ = 0;
};

// EncodeRecord frames payload exactly as WalWriter writes it. Exposed for tests and the
// fuzzer.
std::string EncodeRecord(std::string_view payload);

}  // namespace kv::storage

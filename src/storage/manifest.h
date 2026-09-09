#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace kv::storage {

// ManifestState is the durable description of which files make up the database. It is
// rewritten in full and swapped in with an atomic rename, so the set of live SSTables
// changes all at once: after a crash the store sees the old set or the new one, never a
// half-applied flush or compaction.
struct ManifestState {
  uint64_t next_file_number = 1;
  // Oldest WAL that still holds writes not yet in an SSTable. Recovery replays every log
  // numbered at or above it.
  uint64_t log_number = 0;
  // Live SSTables, newest first. Reads consult them in this order and stop at the first
  // hit, so the order is part of correctness, not just bookkeeping.
  std::vector<uint64_t> tables;
};

std::string EncodeManifest(const ManifestState& state);
Status DecodeManifest(std::string_view data, ManifestState* state);

Status WriteManifest(const std::string& dir, const ManifestState& state);
// ReadManifest returns NotFound for a directory that has never been opened.
Status ReadManifest(const std::string& dir, ManifestState* state);

}  // namespace kv::storage

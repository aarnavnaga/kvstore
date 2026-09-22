#pragma once

// Thin POSIX file wrappers. The storage engine's durability guarantees rest on three
// primitives here: Sync (data reaches stable storage), SyncDir (a create or rename is
// durable), and WriteFileAtomic (readers see the old contents or the new, never a mix).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace kv {

// AppendFile is an append-only file handle with an explicit sync.
class AppendFile {
 public:
  static Status Open(const std::string& path, std::unique_ptr<AppendFile>* out);
  ~AppendFile();

  AppendFile(const AppendFile&) = delete;
  AppendFile& operator=(const AppendFile&) = delete;

  Status Append(std::string_view data);
  // Sync forces appended data to stable storage (fdatasync on Linux, F_FULLFSYNC on macOS,
  // where plain fsync only reaches the drive's volatile cache).
  Status Sync();
  Status Close();
  [[nodiscard]] uint64_t size() const { return size_; }

 private:
  AppendFile(int fd, std::string path, uint64_t size)
      : fd_(fd), path_(std::move(path)), size_(size) {}

  int fd_;
  std::string path_;
  uint64_t size_;
};

// MappedFile maps a whole file read-only. The mapping stays valid after the file is
// unlinked, which is what lets compaction delete a table while readers still hold it.
class MappedFile {
 public:
  static Status Open(const std::string& path, std::unique_ptr<MappedFile>* out);
  ~MappedFile();

  MappedFile(const MappedFile&) = delete;
  MappedFile& operator=(const MappedFile&) = delete;

  [[nodiscard]] std::string_view data() const { return {static_cast<const char*>(base_), size_}; }

 private:
  MappedFile(void* base, size_t size) : base_(base), size_(size) {}

  void* base_;
  size_t size_;
};

Status CreateDirIfMissing(const std::string& dir);
Status SyncDir(const std::string& dir);
Status ReadFileToString(const std::string& path, std::string* out);
// WriteFileAtomic writes data to path.tmp, syncs it, renames it over path and syncs the
// directory, so a crash at any point leaves either the old file or the new one.
Status WriteFileAtomic(const std::string& path, std::string_view data);
Status RemoveFile(const std::string& path);
// RenamePath atomically renames a file or directory (same filesystem).
Status RenamePath(const std::string& from, const std::string& to);
Status RemoveDirRecursive(const std::string& dir);
Status ListDir(const std::string& dir, std::vector<std::string>* names);
bool FileExists(const std::string& path);
Status FileSize(const std::string& path, uint64_t* size);
// DirSize sums the sizes of regular files directly inside dir.
uint64_t DirSize(const std::string& dir);

std::string JoinPath(std::string_view dir, std::string_view name);

}  // namespace kv

#include "common/file.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>

namespace kv {
namespace {

Status SyncFd(int fd, const std::string& what) {
#if defined(__APPLE__)
  if (::fcntl(fd, F_FULLFSYNC) == 0) {
    return Status::OK();
  }
  // Some filesystems (e.g. network mounts) reject F_FULLFSYNC; fall back to fsync.
  if (::fsync(fd) == 0) {
    return Status::OK();
  }
#else
  if (::fdatasync(fd) == 0) {
    return Status::OK();
  }
#endif
  return Status::IOErrorFromErrno("sync " + what);
}

}  // namespace

Status AppendFile::Open(const std::string& path, std::unique_ptr<AppendFile>* out) {
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOErrorFromErrno("open " + path);
  }
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    ::close(fd);
    return Status::IOErrorFromErrno("stat " + path);
  }
  out->reset(new AppendFile(fd, path, static_cast<uint64_t>(st.st_size)));
  return Status::OK();
}

AppendFile::~AppendFile() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

Status AppendFile::Append(std::string_view data) {
  while (!data.empty()) {
    const ssize_t n = ::write(fd_, data.data(), data.size());
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return Status::IOErrorFromErrno("write " + path_);
    }
    data.remove_prefix(static_cast<size_t>(n));
    size_ += static_cast<uint64_t>(n);
  }
  return Status::OK();
}

Status AppendFile::Sync() { return SyncFd(fd_, path_); }

Status AppendFile::Close() {
  if (fd_ < 0) {
    return Status::OK();
  }
  const int rc = ::close(fd_);
  fd_ = -1;
  if (rc != 0) {
    return Status::IOErrorFromErrno("close " + path_);
  }
  return Status::OK();
}

Status MappedFile::Open(const std::string& path, std::unique_ptr<MappedFile>* out) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return Status::IOErrorFromErrno("open " + path);
  }
  struct stat st {};
  if (::fstat(fd, &st) != 0) {
    ::close(fd);
    return Status::IOErrorFromErrno("stat " + path);
  }
  const auto size = static_cast<size_t>(st.st_size);
  void* base = nullptr;
  if (size > 0) {
    base = ::mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
      ::close(fd);
      return Status::IOErrorFromErrno("mmap " + path);
    }
  }
  ::close(fd);  // The mapping holds its own reference to the file.
  out->reset(new MappedFile(base, size));
  return Status::OK();
}

MappedFile::~MappedFile() {
  if (base_ != nullptr) {
    ::munmap(base_, size_);
  }
}

Status CreateDirIfMissing(const std::string& dir) {
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec) {
    return Status::IOError("mkdir " + dir + ": " + ec.message());
  }
  return Status::OK();
}

Status SyncDir(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return Status::IOErrorFromErrno("open dir " + dir);
  }
  Status s = SyncFd(fd, dir);
  ::close(fd);
  return s;
}

Status ReadFileToString(const std::string& path, std::string* out) {
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    if (errno == ENOENT) {
      return Status::NotFound(path);
    }
    return Status::IOErrorFromErrno("open " + path);
  }
  out->clear();
  char buf[64 * 1024];
  while (true) {
    const ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(fd);
      return Status::IOErrorFromErrno("read " + path);
    }
    if (n == 0) {
      break;
    }
    out->append(buf, static_cast<size_t>(n));
  }
  ::close(fd);
  return Status::OK();
}

Status WriteFileAtomic(const std::string& path, std::string_view data) {
  const std::string tmp = path + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    return Status::IOErrorFromErrno("open " + tmp);
  }
  std::string_view rest = data;
  while (!rest.empty()) {
    const ssize_t n = ::write(fd, rest.data(), rest.size());
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ::close(fd);
      return Status::IOErrorFromErrno("write " + tmp);
    }
    rest.remove_prefix(static_cast<size_t>(n));
  }
  Status s = SyncFd(fd, tmp);
  ::close(fd);
  KV_RETURN_IF_ERROR(s);
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    return Status::IOErrorFromErrno("rename " + tmp);
  }
  return SyncDir(std::filesystem::path(path).parent_path().string());
}

Status RemoveFile(const std::string& path) {
  if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
    return Status::IOErrorFromErrno("unlink " + path);
  }
  return Status::OK();
}

Status RemoveDirRecursive(const std::string& dir) {
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  if (ec) {
    return Status::IOError("remove " + dir + ": " + ec.message());
  }
  return Status::OK();
}

Status ListDir(const std::string& dir, std::vector<std::string>* names) {
  names->clear();
  DIR* d = ::opendir(dir.c_str());
  if (d == nullptr) {
    return Status::IOErrorFromErrno("opendir " + dir);
  }
  // readdir is only unsafe when threads share a DIR stream; this one is local.
  while (const dirent* e = ::readdir(d)) {  // NOLINT(concurrency-mt-unsafe)
    const std::string_view name = e->d_name;
    if (name != "." && name != "..") {
      names->emplace_back(name);
    }
  }
  ::closedir(d);
  return Status::OK();
}

bool FileExists(const std::string& path) { return ::access(path.c_str(), F_OK) == 0; }

Status FileSize(const std::string& path, uint64_t* size) {
  struct stat st {};
  if (::stat(path.c_str(), &st) != 0) {
    return Status::IOErrorFromErrno("stat " + path);
  }
  *size = static_cast<uint64_t>(st.st_size);
  return Status::OK();
}

uint64_t DirSize(const std::string& dir) {
  std::vector<std::string> names;
  if (!ListDir(dir, &names).ok()) {
    return 0;
  }
  uint64_t total = 0;
  for (const auto& name : names) {
    uint64_t size = 0;
    if (FileSize(JoinPath(dir, name), &size).ok()) {
      total += size;
    }
  }
  return total;
}

std::string JoinPath(std::string_view dir, std::string_view name) {
  std::string out(dir);
  if (!out.empty() && out.back() != '/') {
    out.push_back('/');
  }
  out.append(name);
  return out;
}

}  // namespace kv

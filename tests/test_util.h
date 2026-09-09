#pragma once

#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>

#include "common/file.h"

namespace kv::test {

// TempDir creates a fresh directory under $TMPDIR (or /tmp) and removes it on scope exit.
class TempDir {
 public:
  TempDir() {
    const char* base = std::getenv("TMPDIR");
    std::string tmpl = std::string(base != nullptr ? base : "/tmp") + "/kvtest-XXXXXX";
    if (::mkdtemp(tmpl.data()) == nullptr) {
      std::abort();
    }
    path_ = tmpl;
  }
  ~TempDir() { (void)RemoveDirRecursive(path_); }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

  [[nodiscard]] const std::string& path() const { return path_; }

 private:
  std::string path_;
};

// TestSeed returns $KV_SEED if set, otherwise a random seed. Tests print the seed they
// used so any failure can be replayed exactly.
inline uint64_t TestSeed() {
  if (const char* s = std::getenv("KV_SEED")) {
    return std::strtoull(s, nullptr, 10);
  }
  return std::random_device{}();
}

// EnvInt reads an integer knob from the environment, e.g. to scale a soak test up.
inline int64_t EnvInt(const char* name, int64_t fallback) {
  if (const char* s = std::getenv(name)) {
    return std::strtoll(s, nullptr, 10);
  }
  return fallback;
}

}  // namespace kv::test

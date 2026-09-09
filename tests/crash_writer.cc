// crash_writer runs a deterministic write workload against a DB and prints each operation
// only after the store has acknowledged it. The crash test SIGKILLs this process at a
// random moment, so everything printed is a write the store promised was durable.
//
// Usage: crash_writer <dir> <first_op>
//
// Operation i puts key<i>; every 7th operation instead deletes key<i-3>.

#include <cstdio>
#include <cstdlib>
#include <string>

#include "storage/db.h"

int main(int argc, char** argv) {
  if (argc != 3) {
    std::fprintf(stderr, "usage: %s <dir> <first_op>\n", argv[0]);
    return 2;
  }
  kv::storage::Options options;
  options.write_buffer_size = 16 << 10;  // flush often so kills land mid-flush too
  std::unique_ptr<kv::storage::DB> db;
  kv::Status s = kv::storage::DB::Open(options, argv[1], &db);
  if (!s.ok()) {
    std::fprintf(stderr, "open: %s\n", s.ToString().c_str());
    return 1;
  }
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  for (long i = std::strtol(argv[2], nullptr, 10);; ++i) {
    if (i % 7 == 0 && i >= 3) {
      s = db->Delete("key" + std::to_string(i - 3));
    } else {
      s = db->Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    if (!s.ok()) {
      std::fprintf(stderr, "op %ld: %s\n", i, s.ToString().c_str());
      return 1;
    }
    std::printf("%ld\n", i);
  }
}

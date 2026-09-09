add_library(kv_common
  common/crc32c.cc
  common/file.cc
  common/status.cc
  common/version.cc
)
target_include_directories(kv_common PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(kv_common PUBLIC Threads::Threads)

add_library(kv_storage
  storage/arena.cc
  storage/bloom.cc
  storage/db.cc
  storage/iterator.cc
  storage/manifest.cc
  storage/memtable.cc
  storage/sstable.cc
  storage/wal.cc
  storage/write_batch.cc
)
target_link_libraries(kv_storage PUBLIC kv_common)

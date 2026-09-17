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

add_library(kv_net
  net/client.cc
  net/frame.cc
  net/protocol.cc
  net/server.cc
)
target_link_libraries(kv_net PUBLIC kv_common)

add_library(kv_client client/kv_client.cc)
target_link_libraries(kv_client PUBLIC kv_net)

add_library(kv_server_lib server/standalone_service.cc)
target_link_libraries(kv_server_lib PUBLIC kv_net kv_storage)

add_executable(kvserver server/main.cc)
target_link_libraries(kvserver PRIVATE kv_server_lib)

add_executable(kvcli tools/kvcli.cc)
target_link_libraries(kvcli PRIVATE kv_client)

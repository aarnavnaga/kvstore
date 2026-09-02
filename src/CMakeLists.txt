add_library(kv_common
  common/version.cc
)
target_include_directories(kv_common PUBLIC ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(kv_common PUBLIC Threads::Threads)

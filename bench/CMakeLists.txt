include(FetchContent)
FetchContent_Declare(hdr_histogram
  URL https://github.com/HdrHistogram/HdrHistogram_c/archive/refs/tags/0.11.8.tar.gz
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(HDR_LOG_REQUIRED OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(hdr_histogram)

add_executable(kvbench kvbench.cc)
target_link_libraries(kvbench PRIVATE kv_client hdr_histogram_static)

add_executable(storage_bench storage_bench.cc)
target_link_libraries(storage_bench PRIVATE kv_storage hdr_histogram_static)

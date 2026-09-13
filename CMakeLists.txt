cmake_minimum_required(VERSION 3.25)
project(kvstore VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

option(KV_BUILD_TESTS "Build unit and integration tests" ON)
option(KV_BUILD_FUZZ "Build libFuzzer targets (clang only)" OFF)
set(KV_SANITIZER "" CACHE STRING "Sanitizer to build with: address, thread or empty")

add_compile_options(-Wall -Wextra -Wpedantic -Wno-missing-field-initializers -Werror=return-type)

if(KV_SANITIZER STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -fno-sanitize-recover=all)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZER STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(KV_BUILD_FUZZ)
  # Instrument every library for coverage; only the fuzz binaries link libFuzzer's main.
  add_compile_options(-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(NOT KV_SANITIZER STREQUAL "")
  message(FATAL_ERROR "Unknown KV_SANITIZER '${KV_SANITIZER}'")
endif()

find_package(Threads REQUIRED)

add_subdirectory(src)

if(KV_BUILD_TESTS)
  enable_testing()
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    add_library(GTest::gtest_main ALIAS gtest_main)
  endif()
  include(GoogleTest)
  add_subdirectory(tests)
endif()

if(KV_BUILD_FUZZ)
  add_subdirectory(fuzz)
endif()

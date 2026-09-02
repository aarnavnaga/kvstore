function(kv_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE kv_common GTest::gtest_main)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 60)
endfunction()

kv_test(version_test version_test.cc)

# Header-only helpers that the tests share (test/support): unique temp
# folders, bounded waits, gRPC servers on free ports and test vectors.
add_library(gvdb_test_support INTERFACE)
add_library(gvdb::test_support ALIAS gvdb_test_support)
target_include_directories(gvdb_test_support INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

# doctest_discover_tests, for gvdb_add_test(... DISCOVER). The script comes
# with the fetched doctest sources, or with the CMake package of a system
# doctest.
FetchContent_GetProperties(doctest)
if(doctest_SOURCE_DIR)
  include("${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake")
else()
  include(doctest)
endif()

add_subdirectory(unit)
add_subdirectory(integration)
add_subdirectory(bench)

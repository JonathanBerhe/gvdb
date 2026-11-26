# Header-only helpers that the tests share (test/support): unique temp
# folders, bounded waits, gRPC servers on free ports and test vectors.
add_library(gvdb_test_support INTERFACE)
add_library(gvdb::test_support ALIAS gvdb_test_support)
target_include_directories(gvdb_test_support INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(unit)
add_subdirectory(integration)
add_subdirectory(bench)

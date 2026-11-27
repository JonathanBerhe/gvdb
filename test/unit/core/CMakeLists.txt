# Unit tests of the core module (src/core). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(core_tests DISCOVER
  SOURCES
    core_test.cpp
  DEPS gvdb::core
  TIMEOUT 60 LABELS unit)

# Unit tests of the common module (src/common). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(common_tests DISCOVER
  SOURCES
    common_test.cpp
  DEPS gvdb::common gvdb::core
  TIMEOUT 60 LABELS unit)

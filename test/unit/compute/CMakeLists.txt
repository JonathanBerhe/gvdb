# Unit tests of the compute module (src/compute). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(compute_tests DISCOVER
  SOURCES
    compute_test.cpp
  DEPS gvdb::compute gvdb::storage gvdb::index gvdb::common gvdb::core
  TIMEOUT 120 LABELS unit)

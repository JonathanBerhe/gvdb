# Unit tests of the config module (src/config). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(config_tests DISCOVER
  SOURCES
    config_test.cpp
  DEPS gvdb::config gvdb::core
  TIMEOUT 30 LABELS unit)

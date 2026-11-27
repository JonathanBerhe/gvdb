# Unit tests of the server module (src/server). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(server_tests DISCOVER
  SOURCES
    server_flags_test.cpp
  DEPS gvdb::server
  TIMEOUT 30 LABELS unit)

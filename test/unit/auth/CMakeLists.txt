# Unit tests of the auth module (src/auth). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(auth_tests DISCOVER
  SOURCES
    rbac_test.cpp
  DEPS gvdb::auth gvdb::config gvdb::common gvdb::core
  TIMEOUT 60 LABELS unit)

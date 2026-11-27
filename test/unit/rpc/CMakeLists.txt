# Unit tests of the rpc module (src/rpc). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(rpc_tests DISCOVER
  SOURCES
    dns_channel_args_test.cpp
    replica_fallback_test.cpp
    auth_processor_test.cpp
    audit_interceptor_test.cpp
  DEPS gvdb::rpc gvdb::service gvdb::config gvdb::auth gvdb::compute gvdb::storage gvdb::index gvdb::common gvdb::core gvdb::proto
  TIMEOUT 60 LABELS unit)

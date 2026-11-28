# Unit tests of the service module (src/service). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(service_tests DISCOVER
  SOURCES
    collection_metadata_cache_test.cpp
    internal_service_metadata_test.cpp
    internal_service_rpc_test.cpp
    proxy_service_test.cpp
    network_test.cpp
    collection_resolver_test.cpp
    vectordb_service_write_test.cpp
    vectordb_service_search_test.cpp
    vectordb_service_import_test.cpp
    vectordb_service_backup_test.cpp
    vectordb_service_distributed_test.cpp
    vectordb_service_modes_test.cpp
  DEPS gvdb::service gvdb::cluster gvdb::consensus gvdb::rpc gvdb::compute gvdb::storage gvdb::index gvdb::common gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

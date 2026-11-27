# Unit tests of the cluster module (src/cluster). One doctest binary.
# CTest runs each test case as its own test.
gvdb_add_test(cluster_tests DISCOVER
  SOURCES
    cluster_simple_test.cpp
    primary_term_tracker_test.cpp
    internal_client_test.cpp
    data_node_test.cpp
    node_registry_test.cpp
    shard_write_gate_test.cpp
    coordinator_backup_test.cpp
    query_node_test.cpp
  DEPS gvdb::cluster gvdb::service gvdb::consensus gvdb::compute gvdb::storage gvdb::index gvdb::common gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

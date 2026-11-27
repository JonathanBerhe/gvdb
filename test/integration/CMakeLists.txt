# Integration tests: several components together, most of them over real
# gRPC servers on free local ports. Each binary runs in a few seconds, so the
# 60 s limits stop a hang early.

gvdb_add_test(raft_auto_join_tests
  SOURCES raft_auto_join_test.cpp
  DEPS gvdb::consensus gvdb::service gvdb::cluster gvdb::core gvdb::proto
  TIMEOUT 60 LABELS integration)
# Raft elections use real-time timeouts that a test cannot inject, so this
# test runs alone: other tests in parallel can delay heartbeats enough to
# start a new election.
set_tests_properties(raft_auto_join_tests PROPERTIES RUN_SERIAL TRUE)

gvdb_add_test(segment_replication_integration_tests
  SOURCES segment_replication_integration_test.cpp
  DEPS gvdb::service gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

gvdb_add_test(distributed_data_node_tests
  SOURCES distributed_data_node_test.cpp
  DEPS gvdb::service gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

gvdb_add_test(proxy_integration_tests
  SOURCES proxy_integration_test.cpp
  DEPS gvdb::service gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

# Coordinator, data node and proxy together.
gvdb_add_test(backup_restore_integration_tests
  SOURCES backup_restore_integration_test.cpp
  DEPS gvdb::service gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

gvdb_add_test(drain_read_fallback_tests
  SOURCES drain_read_fallback_test.cpp
  DEPS gvdb::service gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

# Real gRPC servers for the proxy, the coordinator and two data nodes.
gvdb_add_test(primary_swap_integration_tests
  SOURCES primary_swap_integration_test.cpp
  DEPS gvdb::service gvdb::rpc gvdb::core gvdb::common gvdb::proto
  TIMEOUT 60 LABELS integration)

# The five server programs as real processes. The test needs the programs,
# so building it builds them.
gvdb_add_test(server_programs_tests
  SOURCES server_programs_test.cpp
  DEPS gvdb::proto
  TIMEOUT 300 LABELS integration)
add_dependencies(server_programs_tests
  gvdb-single-node gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy)
target_compile_definitions(server_programs_tests PRIVATE
  GVDB_SINGLE_NODE_BIN="$<TARGET_FILE:gvdb-single-node>"
  GVDB_COORDINATOR_BIN="$<TARGET_FILE:gvdb-coordinator>"
  GVDB_DATA_NODE_BIN="$<TARGET_FILE:gvdb-data-node>"
  GVDB_QUERY_NODE_BIN="$<TARGET_FILE:gvdb-query-node>"
  GVDB_PROXY_BIN="$<TARGET_FILE:gvdb-proxy>")

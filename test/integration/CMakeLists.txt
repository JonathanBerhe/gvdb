# Integration tests: several components together, most of them over real
# gRPC servers on local ports.

gvdb_add_test(consensus_integration_tests
  SOURCES consensus_integration_test.cpp
  DEPS gvdb::consensus gvdb::cluster gvdb::core
  TIMEOUT 300 LABELS integration)

gvdb_add_test(raft_auto_join_tests
  SOURCES raft_auto_join_test.cpp
  DEPS gvdb::consensus gvdb::network gvdb::cluster gvdb::core gvdb::proto
  TIMEOUT 180 LABELS integration)

gvdb_add_test(metadata_sync_integration_tests
  SOURCES metadata_sync_integration_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::core gvdb::proto
  TIMEOUT 300 LABELS integration)

gvdb_add_test(segment_replication_integration_tests
  SOURCES segment_replication_integration_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 300 LABELS integration)

gvdb_add_test(distributed_data_node_tests
  SOURCES distributed_data_node_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 300 LABELS integration)

gvdb_add_test(proxy_integration_tests
  SOURCES proxy_integration_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 300 LABELS integration)

# Coordinator, data node and proxy together.
gvdb_add_test(backup_restore_integration_tests
  SOURCES backup_restore_integration_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::compute gvdb::storage gvdb::index
       gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 120 LABELS integration)

gvdb_add_test(drain_read_fallback_tests
  SOURCES drain_read_fallback_test.cpp
  DEPS gvdb::network gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 60 LABELS integration)

# Each case pays about 10 s of fixture setup (real gRPC servers for the proxy,
# the coordinator and two data nodes).
gvdb_add_test(primary_swap_integration_tests
  SOURCES primary_swap_integration_test.cpp
  DEPS gvdb::network gvdb::core gvdb::utils gvdb::proto
  TIMEOUT 120 LABELS integration)

# Uses InMemoryObjectStore, not a real S3 endpoint.
gvdb_add_test(s3_integration_tests
  SOURCES s3_integration_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::core gvdb::utils
  TIMEOUT 60 LABELS integration)

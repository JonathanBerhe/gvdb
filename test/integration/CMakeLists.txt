# Integration tests - multi-component testing

# Consensus integration tests
add_executable(consensus_integration_tests
  consensus_integration_test.cpp
)

target_link_libraries(consensus_integration_tests
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ConsensusIntegrationTests COMMAND consensus_integration_tests)

# Set test properties
set_tests_properties(ConsensusIntegrationTests PROPERTIES
  TIMEOUT 300  # Integration tests can take longer
  LABELS "integration"
)

# Metadata synchronization integration tests
add_executable(metadata_sync_integration_tests
  metadata_sync_integration_test.cpp
)

target_link_libraries(metadata_sync_integration_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_consensus
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME MetadataSyncIntegrationTests COMMAND metadata_sync_integration_tests)

# Set test properties
set_tests_properties(MetadataSyncIntegrationTests PROPERTIES
  TIMEOUT 300  # Integration tests can take longer
  LABELS "integration"
)

# Segment replication integration tests
add_executable(segment_replication_integration_tests
  segment_replication_integration_test.cpp
)

target_link_libraries(segment_replication_integration_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_consensus
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_core
    gvdb_utils
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME SegmentReplicationIntegrationTests COMMAND segment_replication_integration_tests)

# Set test properties
set_tests_properties(SegmentReplicationIntegrationTests PROPERTIES
  TIMEOUT 300  # Integration tests can take longer
  LABELS "integration"
)

# Distributed data node integration tests
add_executable(distributed_data_node_tests
  distributed_data_node_test.cpp
)

target_link_libraries(distributed_data_node_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_consensus
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_core
    gvdb_utils
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

add_test(NAME DistributedDataNodeTests COMMAND distributed_data_node_tests)

set_tests_properties(DistributedDataNodeTests PROPERTIES
  TIMEOUT 300
  LABELS "integration"
)

# Proxy integration tests
add_executable(proxy_integration_tests
  proxy_integration_test.cpp
)

target_link_libraries(proxy_integration_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_consensus
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_core
    gvdb_utils
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

add_test(NAME ProxyIntegrationTests COMMAND proxy_integration_tests)

set_tests_properties(ProxyIntegrationTests PROPERTIES
  TIMEOUT 300
  LABELS "integration"
)

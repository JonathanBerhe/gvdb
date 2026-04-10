# Unit tests

# Core module tests
add_executable(core_tests
  core_test.cpp
)

target_link_libraries(core_tests
  PRIVATE
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME CoreTests COMMAND core_tests)

# Set test properties
set_tests_properties(CoreTests PROPERTIES
  TIMEOUT 60
)

# Index module tests
add_executable(index_tests
  index_test.cpp
)

target_link_libraries(index_tests
  PRIVATE
    gvdb_index
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME IndexTests COMMAND index_tests)

# Set test properties
set_tests_properties(IndexTests PROPERTIES
  TIMEOUT 120
)

# Sparse index tests
add_executable(sparse_index_tests
  sparse_index_test.cpp
)

target_link_libraries(sparse_index_tests
  PRIVATE
    gvdb_index
    gvdb_core
    doctest::doctest_with_main
)

add_test(NAME SparseIndexTests COMMAND sparse_index_tests)

set_tests_properties(SparseIndexTests PROPERTIES
  TIMEOUT 60
)

# TurboQuant index tests
add_executable(turboquant_tests
  turboquant_test.cpp
)

target_link_libraries(turboquant_tests
  PRIVATE
    gvdb_index
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME TurboQuantTests COMMAND turboquant_tests)

# Set test properties
set_tests_properties(TurboQuantTests PROPERTIES
  TIMEOUT 120
)

# Storage module tests
add_executable(storage_tests
  storage_test.cpp
)

target_link_libraries(storage_tests
  PRIVATE
    gvdb_storage
    gvdb_index
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME StorageTests COMMAND storage_tests)

# Set test properties
set_tests_properties(StorageTests PROPERTIES
  TIMEOUT 120
)

# Object store tests
add_executable(object_store_tests object_store_test.cpp)
target_link_libraries(object_store_tests PRIVATE gvdb_storage gvdb_core doctest::doctest_with_main)
add_test(NAME ObjectStoreTests COMMAND object_store_tests)
set_tests_properties(ObjectStoreTests PROPERTIES TIMEOUT 30)

# Segment cache tests
add_executable(segment_cache_tests segment_cache_test.cpp)
target_link_libraries(segment_cache_tests PRIVATE gvdb_storage gvdb_core doctest::doctest_with_main)
add_test(NAME SegmentCacheTests COMMAND segment_cache_tests)
set_tests_properties(SegmentCacheTests PROPERTIES TIMEOUT 30)

# Tiered segment manager tests
add_executable(tiered_segment_manager_tests tiered_segment_manager_test.cpp)
target_link_libraries(tiered_segment_manager_tests PRIVATE gvdb_storage gvdb_index gvdb_core gvdb_utils doctest::doctest_with_main)
add_test(NAME TieredSegmentManagerTests COMMAND tiered_segment_manager_tests)
set_tests_properties(TieredSegmentManagerTests PROPERTIES TIMEOUT 60)

# Segment manifest tests
add_executable(segment_manifest_tests segment_manifest_test.cpp)
target_link_libraries(segment_manifest_tests PRIVATE gvdb_storage gvdb_core doctest::doctest_with_main)
add_test(NAME SegmentManifestTests COMMAND segment_manifest_tests)
set_tests_properties(SegmentManifestTests PROPERTIES TIMEOUT 30)

# Utils module tests
add_executable(utils_tests
  utils_test.cpp
)

target_link_libraries(utils_tests
  PRIVATE
    gvdb_utils
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME UtilsTests COMMAND utils_tests)

# Set test properties
set_tests_properties(UtilsTests PROPERTIES
  TIMEOUT 60
)

# Compute module tests
add_executable(compute_tests
  compute_test.cpp
)

target_link_libraries(compute_tests
  PRIVATE
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ComputeTests COMMAND compute_tests)

# Set test properties
set_tests_properties(ComputeTests PROPERTIES
  TIMEOUT 120
)

# Network module tests
add_executable(network_tests
  network_test.cpp
)

target_link_libraries(network_tests
  PRIVATE
    gvdb_network
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME NetworkTests COMMAND network_tests)

# Set test properties
set_tests_properties(NetworkTests PROPERTIES
  TIMEOUT 120
)

# Collection metadata cache tests
add_executable(metadata_cache_tests
  collection_metadata_cache_test.cpp
)

target_link_libraries(metadata_cache_tests
  PRIVATE
    gvdb_network
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME MetadataCacheTests COMMAND metadata_cache_tests)

# Set test properties
set_tests_properties(MetadataCacheTests PROPERTIES
  TIMEOUT 60
)

# InternalService metadata RPC tests
add_executable(internal_service_metadata_tests
  internal_service_metadata_test.cpp
)

target_link_libraries(internal_service_metadata_tests
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
add_test(NAME InternalServiceMetadataTests COMMAND internal_service_metadata_tests)

# Set test properties
set_tests_properties(InternalServiceMetadataTests PROPERTIES
  TIMEOUT 60
)

# InternalService RPC tests (comprehensive)
add_executable(internal_service_rpc_tests
  internal_service_rpc_test.cpp
)

target_link_libraries(internal_service_rpc_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_consensus
    gvdb_storage
    gvdb_index
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME InternalServiceRpcTests COMMAND internal_service_rpc_tests)

# Set test properties
set_tests_properties(InternalServiceRpcTests PROPERTIES
  TIMEOUT 120
)

# Cluster module tests (simplified - no storage/compute dependencies)
add_executable(cluster_tests
  cluster_simple_test.cpp
)

target_link_libraries(cluster_tests
  PRIVATE
    gvdb_cluster
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ClusterTests COMMAND cluster_tests)

# Set test properties
set_tests_properties(ClusterTests PROPERTIES
  TIMEOUT 120
)

# Consensus module tests
add_executable(consensus_tests
  consensus_test.cpp
)

target_link_libraries(consensus_tests
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ConsensusTests COMMAND consensus_tests)

# Set test properties
set_tests_properties(ConsensusTests PROPERTIES
  TIMEOUT 120
)

# Consensus persistence tests
add_executable(consensus_persistence_tests
  consensus_persistence_test.cpp
)

target_link_libraries(consensus_persistence_tests
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ConsensusPersistenceTests COMMAND consensus_persistence_tests)

# Set test properties
set_tests_properties(ConsensusPersistenceTests PROPERTIES
  TIMEOUT 180
)

# Proxy service tests
add_executable(proxy_service_tests
  proxy_service_test.cpp
)

target_link_libraries(proxy_service_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME ProxyServiceTests COMMAND proxy_service_tests)

# Set test properties
set_tests_properties(ProxyServiceTests PROPERTIES
  TIMEOUT 120
)

# VectorDBService DISTRIBUTED mode tests
add_executable(vectordb_service_distributed_tests
  vectordb_service_distributed_test.cpp
)

target_link_libraries(vectordb_service_distributed_tests
  PRIVATE
    gvdb_network
    gvdb_storage
    gvdb_compute
    gvdb_index
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME VectorDBServiceDistributedTests COMMAND vectordb_service_distributed_tests)

# Set test properties
set_tests_properties(VectorDBServiceDistributedTests PROPERTIES
  TIMEOUT 120
)

# VectorDBService COORDINATOR mode tests
add_executable(vectordb_service_coordinator_tests
  vectordb_service_coordinator_test.cpp
)

target_link_libraries(vectordb_service_coordinator_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_storage
    gvdb_compute
    gvdb_index
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME VectorDBServiceCoordinatorTests COMMAND vectordb_service_coordinator_tests)

# Set test properties
set_tests_properties(VectorDBServiceCoordinatorTests PROPERTIES
  TIMEOUT 120
)

# InternalClient tests
add_executable(internal_client_tests
  internal_client_test.cpp
)

target_link_libraries(internal_client_tests
  PRIVATE
    gvdb_cluster
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME InternalClientTests COMMAND internal_client_tests)

# Set test properties
set_tests_properties(InternalClientTests PROPERTIES
  TIMEOUT 120
)

# Auth processor tests
add_executable(auth_processor_tests
  auth_processor_test.cpp
)

target_link_libraries(auth_processor_tests
  PRIVATE
    gvdb_network
    gvdb_auth
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

add_test(NAME AuthProcessorTests COMMAND auth_processor_tests)

set_tests_properties(AuthProcessorTests PROPERTIES
  TIMEOUT 60
)

# RBAC unit tests
add_executable(rbac_tests
  rbac_test.cpp
)

target_link_libraries(rbac_tests
  PRIVATE
    gvdb_auth
    gvdb_utils
    gvdb_core
    doctest::doctest_with_main
)

add_test(NAME RbacTests COMMAND rbac_tests)

set_tests_properties(RbacTests PROPERTIES
  TIMEOUT 60
)

# DataNode tests
add_executable(data_node_tests
  data_node_test.cpp
)

target_link_libraries(data_node_tests
  PRIVATE
    gvdb_cluster
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME DataNodeTests COMMAND data_node_tests)

# Set test properties
set_tests_properties(DataNodeTests PROPERTIES
  TIMEOUT 60
)

# Collection resolver tests
add_executable(collection_resolver_tests
  collection_resolver_test.cpp
)

target_link_libraries(collection_resolver_tests
  PRIVATE
    gvdb_network
    gvdb_cluster
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME CollectionResolverTests COMMAND collection_resolver_tests)

# Set test properties
set_tests_properties(CollectionResolverTests PROPERTIES
  TIMEOUT 120
)

# Node registry tests
add_executable(node_registry_tests
  node_registry_test.cpp
)

target_link_libraries(node_registry_tests
  PRIVATE
    gvdb_cluster
    proto_lib
    ${_PROTOBUF_LIBPROTOBUF}
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME NodeRegistryTests COMMAND node_registry_tests)

# Set test properties
set_tests_properties(NodeRegistryTests PROPERTIES
  TIMEOUT 60
)

# QueryNode tests
add_executable(query_node_tests
  query_node_test.cpp
)

target_link_libraries(query_node_tests
  PRIVATE
    gvdb_cluster
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    doctest::doctest_with_main
)

# Add test to CTest
add_test(NAME QueryNodeTests COMMAND query_node_tests)

# Set test properties
set_tests_properties(QueryNodeTests PROPERTIES
  TIMEOUT 120
)

# Unit tests. Each file builds into one doctest executable that CTest runs
# under the same name.

# core
gvdb_add_test(core_tests
  SOURCES core_test.cpp
  DEPS gvdb::core
  TIMEOUT 60 LABELS unit)

# index
gvdb_add_test(index_tests
  SOURCES index_test.cpp
  DEPS gvdb::index gvdb::core
  # Checks that adaptive parameters reach the FAISS wrappers
  # (FaissHNSWIndex::GetEfSearch and others), which are private to the module.
  INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/index
  TIMEOUT 120 LABELS unit)

gvdb_add_test(sparse_index_tests
  SOURCES sparse_index_test.cpp
  DEPS gvdb::index gvdb::core
  TIMEOUT 60 LABELS unit)

gvdb_add_test(turboquant_tests
  SOURCES turboquant_test.cpp
  DEPS gvdb::index gvdb::core
  TIMEOUT 120 LABELS unit)

if(GVDB_WITH_CUDA)
  gvdb_add_test(cuda_smoke_tests
    SOURCES cuda_smoke_test.cpp
    DEPS gvdb::index gvdb::core faiss
    INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/index
    TIMEOUT 120 LABELS unit gpu cuda)
endif()

if(GVDB_WITH_METAL)
  gvdb_add_test(metal_distance_tests
    SOURCES metal_distance_test.cpp
    DEPS gvdb::index gvdb::core gvdb::metal_cpp faiss
    INCLUDE_DIRS ${PROJECT_SOURCE_DIR}/src/index ${PROJECT_SOURCE_DIR}/src/index/metal
    # Loads the kernel source from <build>/kernels.
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}
    TIMEOUT 120 LABELS unit gpu metal)
endif()

# storage
gvdb_add_test(storage_tests
  SOURCES storage_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::core
  TIMEOUT 120 LABELS unit)

gvdb_add_test(object_store_tests
  SOURCES object_store_test.cpp
  DEPS gvdb::storage gvdb::core
  TIMEOUT 30 LABELS unit)

gvdb_add_test(segment_cache_tests
  SOURCES segment_cache_test.cpp
  DEPS gvdb::storage gvdb::core
  TIMEOUT 30 LABELS unit)

gvdb_add_test(tiered_segment_manager_tests
  SOURCES tiered_segment_manager_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::core gvdb::utils
  TIMEOUT 60 LABELS unit)

gvdb_add_test(segment_manifest_tests
  SOURCES segment_manifest_test.cpp
  DEPS gvdb::storage gvdb::core
  TIMEOUT 30 LABELS unit)

gvdb_add_test(bulk_import_tests
  SOURCES bulk_import_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::utils gvdb::core
  TIMEOUT 120 LABELS unit)

gvdb_add_test(backup_manifest_tests
  SOURCES backup_manifest_test.cpp
  DEPS gvdb::storage gvdb::core
  TIMEOUT 30 LABELS unit)

gvdb_add_test(backup_manager_tests
  SOURCES backup_manager_test.cpp
  DEPS gvdb::storage gvdb::index gvdb::utils gvdb::core
  TIMEOUT 60 LABELS unit)

# utils
gvdb_add_test(utils_tests
  SOURCES utils_test.cpp
  DEPS gvdb::utils gvdb::core
  TIMEOUT 60 LABELS unit)

# auth
gvdb_add_test(rbac_tests
  SOURCES rbac_test.cpp
  DEPS gvdb::auth gvdb::utils gvdb::core
  TIMEOUT 60 LABELS unit)

# compute
gvdb_add_test(compute_tests
  SOURCES compute_test.cpp
  DEPS gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core
  TIMEOUT 120 LABELS unit)

# cluster
gvdb_add_test(cluster_tests
  SOURCES cluster_simple_test.cpp
  DEPS gvdb::cluster
  TIMEOUT 120 LABELS unit)

gvdb_add_test(primary_term_tracker_tests
  SOURCES primary_term_tracker_test.cpp
  DEPS gvdb::cluster
  TIMEOUT 10 LABELS unit)

gvdb_add_test(internal_client_tests
  SOURCES internal_client_test.cpp
  DEPS gvdb::cluster gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(data_node_tests
  SOURCES data_node_test.cpp
  DEPS gvdb::cluster
  TIMEOUT 60 LABELS unit)

gvdb_add_test(node_registry_tests
  SOURCES node_registry_test.cpp
  DEPS gvdb::cluster gvdb::proto
  TIMEOUT 60 LABELS unit)

gvdb_add_test(shard_write_gate_tests
  SOURCES shard_write_gate_test.cpp
  DEPS gvdb::cluster gvdb::core
  TIMEOUT 30 LABELS unit)

gvdb_add_test(coordinator_backup_tests
  SOURCES coordinator_backup_test.cpp
  DEPS gvdb::cluster gvdb::storage gvdb::network gvdb::consensus gvdb::core gvdb::proto
  TIMEOUT 60 LABELS unit)

gvdb_add_test(query_node_tests
  SOURCES query_node_test.cpp
  DEPS gvdb::cluster gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core
  TIMEOUT 120 LABELS unit)

# consensus
gvdb_add_test(consensus_tests
  SOURCES consensus_test.cpp
  DEPS gvdb::consensus gvdb::cluster gvdb::core
  TIMEOUT 120 LABELS unit)

gvdb_add_test(consensus_persistence_tests
  SOURCES consensus_persistence_test.cpp
  DEPS gvdb::consensus gvdb::cluster gvdb::core
  TIMEOUT 180 LABELS unit)

# network
gvdb_add_test(network_tests
  SOURCES network_test.cpp
  DEPS gvdb::network gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(metadata_cache_tests
  SOURCES collection_metadata_cache_test.cpp
  DEPS gvdb::network gvdb::core
  TIMEOUT 60 LABELS unit)

gvdb_add_test(internal_service_metadata_tests
  SOURCES internal_service_metadata_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::core gvdb::proto
  TIMEOUT 60 LABELS unit)

gvdb_add_test(internal_service_rpc_tests
  SOURCES internal_service_rpc_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::consensus gvdb::storage gvdb::index gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(proxy_service_tests
  SOURCES proxy_service_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(dns_channel_args_tests
  SOURCES dns_channel_args_test.cpp
  DEPS gvdb::network
  TIMEOUT 30 LABELS unit)

gvdb_add_test(replica_fallback_tests
  SOURCES replica_fallback_test.cpp
  DEPS gvdb::network
  TIMEOUT 30 LABELS unit)

gvdb_add_test(vectordb_service_distributed_tests
  SOURCES vectordb_service_distributed_test.cpp
  DEPS gvdb::network gvdb::storage gvdb::compute gvdb::index gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(vectordb_service_coordinator_tests
  SOURCES vectordb_service_coordinator_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::storage gvdb::compute gvdb::index gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(collection_resolver_tests
  SOURCES collection_resolver_test.cpp
  DEPS gvdb::network gvdb::cluster gvdb::storage gvdb::index gvdb::utils gvdb::core gvdb::proto
  TIMEOUT 120 LABELS unit)

gvdb_add_test(auth_processor_tests
  SOURCES auth_processor_test.cpp
  DEPS gvdb::network gvdb::auth gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core gvdb::proto
  TIMEOUT 60 LABELS unit)

gvdb_add_test(audit_interceptor_tests
  SOURCES audit_interceptor_test.cpp
  DEPS gvdb::network gvdb::auth gvdb::compute gvdb::storage gvdb::index gvdb::utils gvdb::core gvdb::proto
  TIMEOUT 60 LABELS unit)

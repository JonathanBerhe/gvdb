# Cluster Module - Distributed Coordination Layer

add_library(gvdb_cluster
  shard_manager.cpp
  coordinator.cpp
  query_node.cpp
  data_node.cpp
  load_balancer.cpp
  replication.cpp
  node_registry.cpp
  internal_client.cpp
  heartbeat_sender.cpp
)

target_include_directories(gvdb_cluster
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(gvdb_cluster
  PUBLIC
    gvdb_core
    gvdb_index
    proto_lib
    absl::status
    absl::statusor
    absl::strings
    absl::synchronization
  PRIVATE
    gvdb_storage
    gvdb_compute
    gvdb_network
    gvdb_utils
)

# Enable warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(gvdb_cluster PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Main binaries

# All-in-one binary (single-process deployment)
add_executable(gvdb-all-in-one
  all_in_one_main.cpp
)

target_link_libraries(gvdb-all-in-one
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    network
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    absl::strings
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

# Coordinator binary (metadata management and Raft consensus)
add_executable(gvdb-coordinator
  coordinator_main.cpp
)

target_link_libraries(gvdb-coordinator
  PRIVATE
    gvdb_consensus
    gvdb_cluster
    gvdb_utils
    gvdb_core
    absl::strings
    absl::str_format
)

# Data Node binary (storage and indexing)
add_executable(gvdb-data-node
  data_node_main.cpp
)

target_link_libraries(gvdb-data-node
  PRIVATE
    gvdb_cluster
    network
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    absl::strings
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

# Query Node binary (segment caching and query execution)
add_executable(gvdb-query-node
  query_node_main.cpp
)

target_link_libraries(gvdb-query-node
  PRIVATE
    gvdb_cluster
    network
    gvdb_compute
    gvdb_storage
    gvdb_index
    gvdb_utils
    gvdb_core
    proto_lib
    absl::strings
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF}
)

# Proxy binary (lightweight request routing)
add_executable(gvdb-proxy
  proxy_main.cpp
)

target_link_libraries(gvdb-proxy
  PRIVATE
    # Minimal dependencies for routing only
    gvdb_utils        # Logging, metrics
    gvdb_core         # Types, status
    absl::strings
    ${_GRPC_GRPCPP}   # gRPC for forwarding
    ${_PROTOBUF_LIBPROTOBUF}
)

# Set output directory for binaries
set_target_properties(gvdb-all-in-one gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Install targets
install(TARGETS gvdb-all-in-one gvdb-coordinator gvdb-data-node gvdb-query-node gvdb-proxy
  DESTINATION bin
)

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

# Set output directory for binaries
set_target_properties(gvdb-all-in-one
  PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

# Install target
install(TARGETS gvdb-all-in-one
  DESTINATION bin
)

# Network Module - gRPC communication layer

add_library(network
  grpc_server.cpp
  vectordb_service.cpp
  proto_conversions.cpp
)

target_link_libraries(network
  PUBLIC
    core
    gvdb_utils
    proto_lib
  PRIVATE
    gvdb_storage
    gvdb_compute
    absl::status
    absl::statusor
    absl::strings
)

target_include_directories(network
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

# Network Module - gRPC communication layer

add_library(network
  vectordb_service.cpp
  proto_conversions.cpp
  internal_service.cpp
)

target_link_libraries(network
  PUBLIC
    core
    gvdb_utils
    proto_lib
  PRIVATE
    gvdb_storage
    gvdb_compute
    gvdb_cluster
    absl::status
    absl::statusor
    absl::strings
)

target_include_directories(network
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

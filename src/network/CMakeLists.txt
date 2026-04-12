# Network Module - gRPC communication layer

add_library(gvdb_network
  vectordb_service.cpp
  proto_conversions.cpp
  internal_service.cpp
  proxy_service.cpp
  collection_metadata_cache.cpp
  collection_resolver.cpp
  auth_processor.cpp
  audit_interceptor.cpp
  audit_context.cpp
)

target_link_libraries(gvdb_network
  PUBLIC
    gvdb_core
    gvdb_utils
    gvdb_auth
    proto_lib
  PRIVATE
    gvdb_storage
    gvdb_compute
    gvdb_cluster
    gvdb_consensus
    absl::status
    absl::statusor
    absl::strings
)

target_include_directories(gvdb_network
  PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)

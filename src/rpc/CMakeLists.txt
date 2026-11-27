# RPC module: conversions between the protobuf messages and the core, storage
# and meta types, and between absl and gRPC status codes.
gvdb_add_library(rpc
  SOURCES
    proto_conversions.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::meta
    gvdb::storage
    gvdb::proto
    absl::status
    absl::statusor
  PRIVATE_DEPS
    absl::str_format
)

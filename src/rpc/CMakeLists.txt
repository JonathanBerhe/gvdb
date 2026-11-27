# RPC module: conversions between the protobuf messages and the core, storage
# and meta types, gRPC status mapping, channel arguments, the primary term
# header, replica fallback, and the auth and audit server interceptors.
gvdb_add_library(rpc
  SOURCES
    proto_conversions.cpp
    dns_channel_args.cpp
    primary_term_header.cpp
    replica_fallback.cpp
    interceptors/auth_processor.cpp
    interceptors/audit_interceptor.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::auth
    gvdb::meta
    gvdb::storage
    gvdb::proto
    absl::function_ref
    absl::status
    absl::statusor
  PRIVATE_DEPS
    gvdb::common
    absl::str_format
)

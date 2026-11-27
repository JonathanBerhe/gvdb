# Service module: the gRPC services (client API, internal API, proxy) and the
# collection resolvers.
gvdb_add_library(service
  SOURCES
    vectordb_service.cpp
    internal_service.cpp
    proxy_service.cpp
    collection_metadata_cache.cpp
    collection_resolver.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::auth
    gvdb::meta
    gvdb::storage
    gvdb::compute
    gvdb::cluster
    gvdb::proto
  PRIVATE_DEPS
    gvdb::common
    gvdb::index
    gvdb::rpc
    gvdb::consensus
    absl::status
    absl::statusor
    absl::strings
)

# Service module: the gRPC services.
#   vectordb/  the client API (VectorDBService), one source per handler group,
#              and the collection resolvers
#   internal/  the cluster-internal API (InternalService): coordinator RPCs and
#              data node RPCs
#   proxy/     the proxy (ProxyService)
gvdb_add_library(service
  SOURCES
    vectordb/vectordb_service.cpp
    vectordb/request_guard.cpp
    vectordb/segment_locator.cpp
    vectordb/write_path.cpp
    vectordb/read_path.cpp
    vectordb/admin_handlers.cpp
    vectordb/write_handlers.cpp
    vectordb/read_handlers.cpp
    vectordb/distributed_search.cpp
    vectordb/import_handlers.cpp
    vectordb/backup_handlers.cpp
    vectordb/collection_metadata_cache.cpp
    vectordb/collection_resolver.cpp
    internal/internal_service.cpp
    internal/coordinator_handlers.cpp
    internal/data_node_handlers.cpp
    proxy/proxy_service.cpp
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

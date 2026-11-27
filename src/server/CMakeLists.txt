# Server module: the steps that the server programs in src/main share. The
# flag table, the startup and shutdown steps, the builders that turn the YAML
# configuration into storage, backup and interceptors, and the cluster steps
# (Raft, heartbeat, drain).
gvdb_add_library(server
  SOURCES
    bootstrap.cpp
    builders.cpp
    cluster_steps.cpp
    flags.cpp
  PUBLIC_DEPS
    gvdb::config
    gvdb::cluster
    gvdb::consensus
    gvdb::storage
    gvdb::index
    gvdb::auth
    gvdb::common
    gvdb::proto
    absl::status
    absl::statusor
    grpc++
  PRIVATE_DEPS
    gvdb::rpc
    gvdb::core
    absl::strings
)

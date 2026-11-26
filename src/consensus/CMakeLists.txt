# Consensus module: Raft (NuRaft) with a RocksDB log store, and the
# timestamp oracle.
gvdb_add_library(consensus
  SOURCES
    timestamp_oracle.cpp
    metadata_store.cpp
    metadata_state_machine.cpp
    gvdb_log_store.cpp
    gvdb_state_manager.cpp
    raft_node.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::cluster
    absl::status
    absl::statusor
    absl::strings
    absl::synchronization
    NuRaft::static_lib
    rocksdb
  PRIVATE_DEPS
    gvdb::utils
    # The startup peer probe and AnnounceJoinToCluster call the internal
    # gRPC API (raft_node.cpp).
    gvdb::proto
    grpc++
)

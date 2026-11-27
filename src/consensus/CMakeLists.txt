# Consensus module: Raft (NuRaft) with a RocksDB log store, and the
# timestamp oracle. The calls to other coordinators go through
# IRaftPeerClient (raft_peer_client.h), which the cluster module implements
# over gRPC.
gvdb_add_library(consensus
  SOURCES
    timestamp_oracle.cpp
    metadata_state_machine.cpp
    gvdb_log_store.cpp
    gvdb_state_manager.cpp
    raft_node.cpp
    raft_join.cpp
    nuraft_logger.cpp
  PUBLIC_DEPS
    gvdb::core
    absl::status
    absl::statusor
    absl::strings
    NuRaft::static_lib
    rocksdb
  PRIVATE_DEPS
    gvdb::common
)

# Cluster module: the cluster roles.
#   coordinator/  the Coordinator (one source per responsibility), the node
#                 registry, the client that calls the data nodes and the
#                 gRPC client that Raft uses to reach the other coordinators
#   datanode/     the index build queue, the heartbeat, the primary-term
#                 tracker and the backup write fence of a data node
#   shard_manager the shard map that both roles use
#   query_node    segment loading for a query node
gvdb_add_library(cluster
  SOURCES
    coordinator/coordinator.cpp
    coordinator/nodes.cpp
    coordinator/collections.cpp
    coordinator/health_checks.cpp
    coordinator/replication.cpp
    coordinator/rebalance.cpp
    coordinator/primary_swap.cpp
    coordinator/backup_orchestration.cpp
    coordinator/registry_persistence.cpp
    coordinator/node_registry.cpp
    coordinator/internal_client.cpp
    coordinator/raft_peer_client.cpp
    datanode/data_node.cpp
    datanode/heartbeat_sender.cpp
    datanode/primary_term_tracker.cpp
    datanode/shard_write_gate.cpp
    shard_manager.cpp
    query_node.cpp
  PUBLIC_DEPS
    gvdb::core
    gvdb::consensus
    gvdb::index
    gvdb::storage
    gvdb::compute
    gvdb::meta
    gvdb::proto
    absl::status
    absl::statusor
    absl::strings
  PRIVATE_DEPS
    gvdb::rpc
    gvdb::common
)

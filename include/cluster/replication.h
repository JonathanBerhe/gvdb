// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "core/vector.h"
#include "absl/status/status.h"
#include <vector>
#include <future>

namespace gvdb {
namespace cluster {

// Consistency level for writes
enum class ConsistencyLevel {
  ONE,     // Write to one node only
  QUORUM,  // Write to majority of replicas
  ALL      // Write to all replicas
};

// Replication manager handles data replication
class ReplicationManager {
 public:
  ReplicationManager() = default;
  ~ReplicationManager() = default;

  // Replicate write to multiple nodes
  absl::Status ReplicateWrite(
      core::ShardId shard_id,
      const std::vector<core::NodeId>& replica_nodes,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids,
      ConsistencyLevel level);

  // Read repair - fix inconsistencies during reads
  absl::Status ReadRepair(
      core::ShardId shard_id,
      const std::vector<core::NodeId>& replica_nodes);

  // Replicate entire shard to a new node
  absl::Status ReplicateShard(
      core::ShardId shard_id,
      core::NodeId source_node,
      core::NodeId target_node);

 private:
  // Send write to a single node (async)
  std::future<absl::Status> SendWriteToNode(
      core::NodeId node_id,
      core::ShardId shard_id,
      const std::vector<core::Vector>& vectors,
      const std::vector<core::VectorId>& ids);

  // Calculate required acknowledgments based on consistency level
  size_t GetRequiredAcks(ConsistencyLevel level, size_t replica_count) const;
};

}  // namespace cluster
}  // namespace gvdb
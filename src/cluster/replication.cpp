// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/replication.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"
#include <chrono>
#include <thread>

namespace gvdb {
namespace cluster {

namespace {
constexpr uint64_t kWriteTimeoutMs = 5000;  // 5 seconds
}

absl::Status ReplicationManager::ReplicateWrite(
    core::ShardId shard_id,
    const std::vector<core::NodeId>& replica_nodes,
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids,
    ConsistencyLevel level) {

  if (replica_nodes.empty()) {
    return absl::FailedPreconditionError("No replica nodes provided");
  }

  size_t required_acks = GetRequiredAcks(level, replica_nodes.size());

  utils::Logger::Instance().Info(
      "Replicating write to shard {} across {} nodes (required acks: {})",
      core::ToUInt16(shard_id),
      replica_nodes.size(),
      required_acks);

  // Send writes to all replicas asynchronously
  std::vector<std::future<absl::Status>> futures;
  futures.reserve(replica_nodes.size());

  for (const auto& node : replica_nodes) {
    futures.push_back(SendWriteToNode(node, shard_id, vectors, ids));
  }

  // Wait for required acknowledgments
  size_t acks = 0;
  auto deadline = std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(kWriteTimeoutMs);

  for (auto& future : futures) {
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - std::chrono::steady_clock::now());

    if (remaining.count() <= 0) {
      return absl::DeadlineExceededError("Write timeout");
    }

    if (future.wait_for(remaining) == std::future_status::ready) {
      auto status = future.get();
      if (status.ok()) {
        ++acks;
        if (acks >= required_acks) {
          return absl::OkStatus();
        }
      } else {
        utils::Logger::Instance().Warn("Write failed to replica: {}", status.message());
      }
    }
  }

  if (acks < required_acks) {
    return absl::DeadlineExceededError(
        absl::StrCat("Insufficient acknowledgments: got ", acks, ", required ", required_acks));
  }

  return absl::OkStatus();
}

absl::Status ReplicationManager::ReadRepair(
    core::ShardId shard_id,
    const std::vector<core::NodeId>& replica_nodes) {

  utils::Logger::Instance().Info("Performing read repair for shard {}",
                                 core::ToUInt16(shard_id));

  // TODO: Implement read repair
  // This would involve:
  // 1. Read from all replicas
  // 2. Compare versions/timestamps
  // 3. Identify inconsistencies
  // 4. Repair divergent replicas

  return absl::UnimplementedError("Read repair not implemented");
}

absl::Status ReplicationManager::ReplicateShard(
    core::ShardId shard_id,
    core::NodeId source_node,
    core::NodeId target_node) {

  utils::Logger::Instance().Info("Replicating shard {} from node {} to node {}",
                                 core::ToUInt16(shard_id),
                                 core::ToUInt32(source_node),
                                 core::ToUInt32(target_node));

  // TODO: Implement shard replication
  // This would involve:
  // 1. Stream data from source to target
  // 2. Build index on target
  // 3. Verify data integrity
  // 4. Update shard mapping

  return absl::UnimplementedError("Shard replication not implemented");
}

std::future<absl::Status> ReplicationManager::SendWriteToNode(
    core::NodeId node_id,
    core::ShardId shard_id,
    const std::vector<core::Vector>& vectors,
    const std::vector<core::VectorId>& ids) {

  return std::async(std::launch::async, [=]() -> absl::Status {
    // TODO: Implement actual network write via gRPC
    // For now, simulate a successful write
    utils::Logger::Instance().Info("Sending write to node {} for shard {}",
                                   core::ToUInt32(node_id),
                                   core::ToUInt16(shard_id));

    // Simulate network delay
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    return absl::OkStatus();
  });
}

size_t ReplicationManager::GetRequiredAcks(ConsistencyLevel level,
                                           size_t replica_count) const {
  switch (level) {
    case ConsistencyLevel::ONE:
      return 1;

    case ConsistencyLevel::QUORUM:
      return (replica_count / 2) + 1;

    case ConsistencyLevel::ALL:
      return replica_count;

    default:
      return 1;
  }
}

}  // namespace cluster
}  // namespace gvdb
// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/shard_manager.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"
#include <algorithm>
#include <cmath>
#include <mutex>
#include <numeric>

namespace gvdb {
namespace cluster {

namespace {
// FNV-1a hash function for consistent hashing
constexpr uint64_t kFNVOffsetBasis = 14695981039346656037ULL;
constexpr uint64_t kFNVPrime = 1099511628211ULL;

uint64_t FNV1aHash(const void* data, size_t len) {
  uint64_t hash = kFNVOffsetBasis;
  const uint8_t* bytes = static_cast<const uint8_t*>(data);
  for (size_t i = 0; i < len; ++i) {
    hash ^= bytes[i];
    hash *= kFNVPrime;
  }
  return hash;
}

}  // anonymous namespace

ShardManager::ShardManager(size_t num_shards, ShardingStrategy strategy)
    : num_shards_(num_shards), strategy_(strategy) {
  InitializeShards();
  utils::Logger::Instance().Info("ShardManager initialized with {} shards, strategy: {}",
                                 num_shards, static_cast<int>(strategy));
}

void ShardManager::InitializeShards() {
  std::unique_lock lock(shard_mutex_);
  for (size_t i = 0; i < num_shards_; ++i) {
    ShardInfo info;
    info.shard_id = core::MakeShardId(static_cast<uint16_t>(i));
    info.state = ShardState::ACTIVE;
    shards_[info.shard_id] = info;
  }
  RebuildHashRing();
}

void ShardManager::RebuildHashRing() {
  // Must be called with shard_mutex_ held
  hash_ring_.clear();
  for (const auto& [shard_id, info] : shards_) {
    uint16_t sid = core::ToUInt16(shard_id);
    for (int v = 0; v < kVirtualNodesPerShard; ++v) {
      // Create unique key for each virtual node: shard_id * 10000 + v
      uint64_t vnode_key = static_cast<uint64_t>(sid) * 10000 + v;
      uint64_t hash = FNV1aHash(&vnode_key, sizeof(vnode_key));
      hash_ring_[hash] = shard_id;
    }
  }
}

core::ShardId ShardManager::LookupRing(uint64_t hash) const {
  // Find the first virtual node with hash >= input hash (clockwise walk)
  auto it = hash_ring_.lower_bound(hash);
  if (it == hash_ring_.end()) {
    it = hash_ring_.begin();  // Wrap around
  }
  return it->second;
}

size_t ShardManager::HashKey(uint64_t key) const {
  return FNV1aHash(&key, sizeof(key)) % num_shards_;
}

size_t ShardManager::HashKey(const std::string& key) const {
  return FNV1aHash(key.data(), key.size()) % num_shards_;
}

core::ShardId ShardManager::AssignShard(core::VectorId vector_id) const {
  uint64_t id = core::ToUInt64(vector_id);

  switch (strategy_) {
    case ShardingStrategy::HASH: {
      // Use consistent hash ring
      std::shared_lock lock(shard_mutex_);
      if (!hash_ring_.empty()) {
        uint64_t hash = FNV1aHash(&id, sizeof(id));
        return LookupRing(hash);
      }
      return core::MakeShardId(static_cast<uint16_t>(HashKey(id)));
    }

    case ShardingStrategy::ROUND_ROBIN:
      return core::MakeShardId(static_cast<uint16_t>(id % num_shards_));

    case ShardingStrategy::RANGE:
      return core::MakeShardId(static_cast<uint16_t>(HashKey(id)));

    default:
      return core::MakeShardId(static_cast<uint16_t>(HashKey(id)));
  }
}

core::ShardId ShardManager::AssignShard(const std::string& key) const {
  switch (strategy_) {
    case ShardingStrategy::HASH:
      return core::MakeShardId(static_cast<uint16_t>(HashKey(key)));

    case ShardingStrategy::ROUND_ROBIN:
      // For string keys, use hash for round-robin
      return core::MakeShardId(static_cast<uint16_t>(HashKey(key)));

    case ShardingStrategy::RANGE:
      return core::MakeShardId(static_cast<uint16_t>(HashKey(key)));

    default:
      return core::MakeShardId(static_cast<uint16_t>(HashKey(key)));
  }
}

absl::StatusOr<ShardInfo> ShardManager::GetShardInfo(core::ShardId shard_id) const {
  std::shared_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }
  return it->second;
}

std::vector<ShardInfo> ShardManager::GetAllShards() const {
  std::shared_lock lock(shard_mutex_);
  std::vector<ShardInfo> result;
  result.reserve(shards_.size());
  for (const auto& [shard_id, info] : shards_) {
    result.push_back(info);
  }
  return result;
}

std::vector<ShardInfo> ShardManager::GetShardsForNode(core::NodeId node_id) const {
  std::shared_lock lock(shard_mutex_);
  std::vector<ShardInfo> result;

  for (const auto& [shard_id, info] : shards_) {
    if (info.primary_node == node_id) {
      result.push_back(info);
    } else {
      for (const auto& replica : info.replica_nodes) {
        if (replica == node_id) {
          result.push_back(info);
          break;
        }
      }
    }
  }

  return result;
}

absl::StatusOr<core::NodeId> ShardManager::GetPrimaryNode(core::ShardId shard_id) const {
  std::shared_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }

  if (it->second.primary_node == core::kInvalidNodeId) {
    return absl::FailedPreconditionError(
        absl::StrCat("Shard has no primary node: ", core::ToUInt16(shard_id)));
  }

  return it->second.primary_node;
}

absl::StatusOr<std::vector<core::NodeId>> ShardManager::GetReplicaNodes(
    core::ShardId shard_id) const {
  std::shared_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }
  return it->second.replica_nodes;
}

absl::Status ShardManager::SetPrimaryNode(core::ShardId shard_id, core::NodeId node_id) {
  std::unique_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }

  it->second.primary_node = node_id;
  utils::Logger::Instance().Info("Set primary node for shard {} to node {}",
                                 core::ToUInt16(shard_id),
                                 core::ToUInt32(node_id));
  return absl::OkStatus();
}

absl::Status ShardManager::AddReplica(core::ShardId shard_id, core::NodeId node_id) {
  std::unique_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }

  // Check if already a replica
  for (const auto& replica : it->second.replica_nodes) {
    if (replica == node_id) {
      return absl::AlreadyExistsError(
          absl::StrCat("Node ", core::ToUInt32(node_id),
                      " is already a replica for shard ", core::ToUInt16(shard_id)));
    }
  }

  it->second.replica_nodes.push_back(node_id);
  utils::Logger::Instance().Info("Added replica node {} for shard {}",
                                 core::ToUInt32(node_id),
                                 core::ToUInt16(shard_id));
  return absl::OkStatus();
}

absl::Status ShardManager::RemoveReplica(core::ShardId shard_id, core::NodeId node_id) {
  std::unique_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }

  auto& replicas = it->second.replica_nodes;
  auto replica_it = std::find(replicas.begin(), replicas.end(), node_id);
  if (replica_it == replicas.end()) {
    return absl::NotFoundError(
        absl::StrCat("Node ", core::ToUInt32(node_id),
                    " is not a replica for shard ", core::ToUInt16(shard_id)));
  }

  replicas.erase(replica_it);
  utils::Logger::Instance().Info("Removed replica node {} from shard {}",
                                 core::ToUInt32(node_id),
                                 core::ToUInt16(shard_id));
  return absl::OkStatus();
}

absl::Status ShardManager::RegisterNode(core::NodeId node_id) {
  std::unique_lock lock(node_mutex_);
  if (assigned_nodes_.count(node_id) > 0) {
    return absl::AlreadyExistsError(
        absl::StrCat("Node already registered: ", core::ToUInt32(node_id)));
  }

  assigned_nodes_.insert(node_id);
  utils::Logger::Instance().Info("Registered node {}", core::ToUInt32(node_id));
  return absl::OkStatus();
}

absl::Status ShardManager::UnregisterNode(core::NodeId node_id, bool graceful) {
  std::unique_lock node_lock(node_mutex_);
  if (assigned_nodes_.count(node_id) == 0) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  assigned_nodes_.erase(node_id);

  if (graceful) {
    // TODO: Implement graceful shutdown - migrate shards to other nodes
    utils::Logger::Instance().Info("Gracefully unregistering node {}",
                                   core::ToUInt32(node_id));
  } else {
    utils::Logger::Instance().Warn("Force unregistering node {}",
                                   core::ToUInt32(node_id));
  }

  return absl::OkStatus();
}

std::vector<core::NodeId> ShardManager::GetAllNodes() const {
  std::shared_lock lock(node_mutex_);
  return std::vector<core::NodeId>(assigned_nodes_.begin(), assigned_nodes_.end());
}

std::vector<ShardManager::RebalanceMove> ShardManager::CalculateRebalancePlan() {
  std::vector<RebalanceMove> moves;

  // Get all nodes and their current loads
  auto nodes = GetAllNodes();
  if (nodes.size() < 2) {
    // Nothing to rebalance with less than 2 nodes
    return moves;
  }

  // Calculate average load
  std::map<core::NodeId, size_t> node_loads;
  for (const auto& node : nodes) {
    node_loads[node] = GetNodeLoad(node);
  }

  size_t total_load = 0;
  for (const auto& [node, load] : node_loads) {
    total_load += load;
  }

  size_t avg_load = total_load / nodes.size();

  // Find overloaded and underloaded nodes
  std::vector<core::NodeId> overloaded;
  std::vector<core::NodeId> underloaded;

  for (const auto& [node, load] : node_loads) {
    if (load > avg_load * 1.2) {  // 20% threshold
      overloaded.push_back(node);
    } else if (load < avg_load * 0.8) {
      underloaded.push_back(node);
    }
  }

  // Create moves from overloaded to underloaded nodes
  // TODO: Implement actual rebalancing logic based on shard weights

  return moves;
}

absl::Status ShardManager::ExecuteRebalanceMove(const RebalanceMove& move) {
  // TODO: Implement actual shard migration
  // This would involve:
  // 1. Set shard state to MIGRATING
  // 2. Replicate data to target node
  // 3. Update shard mapping
  // 4. Remove from source node
  // 5. Set shard state back to ACTIVE

  utils::Logger::Instance().Info("Executing rebalance move: shard {} from node {} to node {}",
                                 core::ToUInt16(move.shard_id),
                                 core::ToUInt32(move.source_node),
                                 core::ToUInt32(move.target_node));

  return absl::UnimplementedError("Rebalance execution not implemented");
}

float ShardManager::CalculateImbalance() const {
  auto nodes = GetAllNodes();
  if (nodes.size() < 2) {
    return 0.0f;
  }

  std::vector<size_t> loads;
  loads.reserve(nodes.size());
  for (const auto& node : nodes) {
    loads.push_back(GetNodeLoad(node));
  }

  // Calculate standard deviation
  float avg = static_cast<float>(std::accumulate(loads.begin(), loads.end(), 0UL)) / loads.size();

  float variance = 0.0f;
  for (size_t load : loads) {
    float diff = static_cast<float>(load) - avg;
    variance += diff * diff;
  }
  variance /= loads.size();

  return std::sqrt(variance);
}

absl::Status ShardManager::UpdateShardMetrics(core::ShardId shard_id,
                                               size_t data_size,
                                               size_t vector_count,
                                               uint64_t query_count) {
  std::unique_lock lock(shard_mutex_);
  auto it = shards_.find(shard_id);
  if (it == shards_.end()) {
    return absl::NotFoundError(absl::StrCat("Shard not found: ", core::ToUInt16(shard_id)));
  }

  it->second.data_size_bytes = data_size;
  it->second.vector_count = vector_count;
  it->second.query_count = query_count;

  return absl::OkStatus();
}

std::vector<core::NodeId> ShardManager::GetLeastLoadedNodes(size_t count) const {
  auto nodes = GetAllNodes();

  std::sort(nodes.begin(), nodes.end(), [this](core::NodeId a, core::NodeId b) {
    return GetNodeLoad(a) < GetNodeLoad(b);
  });

  if (nodes.size() <= count) {
    return nodes;
  }

  return std::vector<core::NodeId>(nodes.begin(), nodes.begin() + count);
}

size_t ShardManager::GetNodeLoad(core::NodeId node_id) const {
  std::shared_lock lock(shard_mutex_);
  size_t total_load = 0;

  for (const auto& [shard_id, info] : shards_) {
    if (info.primary_node == node_id) {
      // Primary shards have full weight
      total_load += info.data_size_bytes;
    } else {
      // Replicas have partial weight (query load)
      for (const auto& replica : info.replica_nodes) {
        if (replica == node_id) {
          total_load += info.data_size_bytes / 2;  // Half weight for replicas
          break;
        }
      }
    }
  }

  return total_load;
}

}  // namespace cluster
}  // namespace gvdb
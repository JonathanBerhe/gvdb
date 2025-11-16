#include "cluster/coordinator.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"
#include <chrono>
#include <thread>

namespace gvdb {
namespace cluster {

namespace {
constexpr uint64_t kHealthCheckIntervalMs = 5000;  // 5 seconds
constexpr uint64_t kNodeTimeoutMs = 30000;         // 30 seconds
}  // anonymous namespace

Coordinator::Coordinator(std::shared_ptr<ShardManager> shard_manager)
    : shard_manager_(std::move(shard_manager)) {
  utils::Logger::Instance().Info("Coordinator initialized");
}

Coordinator::~Coordinator() {
  Shutdown();
}

absl::Status Coordinator::Start(const std::string& bind_address) {
  utils::Logger::Instance().Info("Starting coordinator on {}", bind_address);

  running_.store(true, std::memory_order_release);
  StartHealthCheckLoop();

  return absl::OkStatus();
}

absl::Status Coordinator::Shutdown() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return absl::OkStatus();  // Already shut down
  }

  utils::Logger::Instance().Info("Shutting down coordinator");
  StopHealthCheckLoop();

  return absl::OkStatus();
}

core::NodeId Coordinator::AllocateNodeId() {
  return core::MakeNodeId(next_node_id_.fetch_add(1, std::memory_order_relaxed));
}

core::CollectionId Coordinator::AllocateCollectionId() {
  return core::MakeCollectionId(next_collection_id_.fetch_add(1, std::memory_order_relaxed));
}

core::Timestamp Coordinator::GetCurrentTimestamp() const {
  auto now = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             now.time_since_epoch())
      .count();
}

absl::Status Coordinator::RegisterNode(const NodeInfo& node_info) {
  std::unique_lock lock(node_mutex_);

  // Check if node already exists
  if (nodes_.count(node_info.node_id) > 0) {
    return absl::AlreadyExistsError(
        absl::StrCat("Node already registered: ", core::ToUInt32(node_info.node_id)));
  }

  NodeInfo info = node_info;
  info.last_heartbeat_ts = GetCurrentTimestamp();

  nodes_[node_info.node_id] = info;

  // Register with shard manager
  auto status = shard_manager_->RegisterNode(node_info.node_id);
  if (!status.ok()) {
    nodes_.erase(node_info.node_id);
    return status;
  }

  utils::Logger::Instance().Info("Registered node {} (type: {}, address: {})",
                                 core::ToUInt32(node_info.node_id),
                                 static_cast<int>(node_info.type),
                                 node_info.address);

  return absl::OkStatus();
}

absl::Status Coordinator::UnregisterNode(core::NodeId node_id) {
  std::unique_lock lock(node_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  nodes_.erase(it);

  // Unregister from shard manager
  shard_manager_->UnregisterNode(node_id, false);

  utils::Logger::Instance().Info("Unregistered node {}", core::ToUInt32(node_id));

  return absl::OkStatus();
}

absl::Status Coordinator::UpdateNodeStatus(core::NodeId node_id, NodeStatus status) {
  std::unique_lock lock(node_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  it->second.status = status;
  utils::Logger::Instance().Info("Updated node {} status to {}",
                                 core::ToUInt32(node_id),
                                 static_cast<int>(status));

  return absl::OkStatus();
}

absl::Status Coordinator::ProcessHeartbeat(core::NodeId node_id, const NodeInfo& node_info) {
  std::unique_lock lock(node_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  // Update node info
  it->second.status = node_info.status;
  it->second.memory_used_bytes = node_info.memory_used_bytes;
  it->second.disk_used_bytes = node_info.disk_used_bytes;
  it->second.cpu_usage_percent = node_info.cpu_usage_percent;
  it->second.last_heartbeat_ts = GetCurrentTimestamp();

  return absl::OkStatus();
}

absl::StatusOr<NodeInfo> Coordinator::GetNodeInfo(core::NodeId node_id) const {
  std::shared_lock lock(node_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  return it->second;
}

std::vector<NodeInfo> Coordinator::GetAllNodes() const {
  std::shared_lock lock(node_mutex_);
  std::vector<NodeInfo> result;
  result.reserve(nodes_.size());

  for (const auto& [node_id, info] : nodes_) {
    result.push_back(info);
  }

  return result;
}

std::vector<NodeInfo> Coordinator::GetHealthyNodes(NodeType type) const {
  std::shared_lock lock(node_mutex_);
  std::vector<NodeInfo> result;

  for (const auto& [node_id, info] : nodes_) {
    if (info.type == type && info.status == NodeStatus::HEALTHY) {
      result.push_back(info);
    }
  }

  return result;
}

absl::StatusOr<core::CollectionId> Coordinator::CreateCollection(
    const std::string& name,
    core::Dimension dimension,
    core::MetricType metric_type,
    core::IndexType index_type,
    size_t replication_factor) {

  std::unique_lock lock(collection_mutex_);

  // Check if collection already exists
  if (collection_name_to_id_.count(name) > 0) {
    return absl::AlreadyExistsError(absl::StrCat("Collection already exists: ", name));
  }

  // Allocate collection ID
  core::CollectionId collection_id = AllocateCollectionId();

  // Create metadata
  CollectionMetadata metadata;
  metadata.collection_id = collection_id;
  metadata.collection_name = name;
  metadata.dimension = dimension;
  metadata.metric_type = metric_type;
  metadata.index_type = index_type;
  metadata.replication_factor = replication_factor;
  metadata.created_at = GetCurrentTimestamp();
  metadata.updated_at = metadata.created_at;

  // Store metadata
  collections_[collection_id] = metadata;
  collection_name_to_id_[name] = collection_id;

  utils::Logger::Instance().Info("Created collection '{}' with ID {} (dim: {}, metric: {}, index: {})",
                                 name,
                                 core::ToUInt32(collection_id),
                                 dimension,
                                 static_cast<int>(metric_type),
                                 static_cast<int>(index_type));

  // Assign shards
  // For Phase 1, use a simple strategy: 1 shard per collection
  size_t num_shards = 1;
  auto assign_status = AssignShardsToCollection(collection_id, num_shards, replication_factor);
  if (!assign_status.ok()) {
    // Rollback
    collections_.erase(collection_id);
    collection_name_to_id_.erase(name);
    return assign_status;
  }

  return collection_id;
}

absl::Status Coordinator::DropCollection(const std::string& name) {
  std::unique_lock lock(collection_mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  core::CollectionId collection_id = name_it->second;
  collections_.erase(collection_id);
  collection_name_to_id_.erase(name_it);

  utils::Logger::Instance().Info("Dropped collection '{}'", name);

  return absl::OkStatus();
}

absl::Status Coordinator::DropCollection(core::CollectionId collection_id) {
  std::unique_lock lock(collection_mutex_);

  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(collection_id)));
  }

  std::string name = it->second.collection_name;
  collections_.erase(it);
  collection_name_to_id_.erase(name);

  utils::Logger::Instance().Info("Dropped collection ID {}", core::ToUInt32(collection_id));

  return absl::OkStatus();
}

absl::StatusOr<CollectionMetadata> Coordinator::GetCollectionMetadata(
    const std::string& name) const {
  std::shared_lock lock(collection_mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  auto it = collections_.find(name_it->second);
  if (it == collections_.end()) {
    return absl::InternalError("Collection metadata inconsistency");
  }

  return it->second;
}

absl::StatusOr<CollectionMetadata> Coordinator::GetCollectionMetadata(
    core::CollectionId id) const {
  std::shared_lock lock(collection_mutex_);

  auto it = collections_.find(id);
  if (it == collections_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(id)));
  }

  return it->second;
}

std::vector<CollectionMetadata> Coordinator::ListCollections() const {
  std::shared_lock lock(collection_mutex_);
  std::vector<CollectionMetadata> result;
  result.reserve(collections_.size());

  for (const auto& [collection_id, metadata] : collections_) {
    result.push_back(metadata);
  }

  return result;
}

absl::Status Coordinator::AssignShardsToCollection(core::CollectionId collection_id,
                                                    size_t num_shards,
                                                    size_t replication_factor) {
  // Get healthy data nodes
  auto data_nodes = GetHealthyNodes(NodeType::DATA_NODE);
  if (data_nodes.empty()) {
    return absl::FailedPreconditionError("No healthy data nodes available");
  }

  if (data_nodes.size() < replication_factor) {
    return absl::FailedPreconditionError(
        absl::StrCat("Not enough data nodes for replication factor ",
                    replication_factor, " (have ", data_nodes.size(), " nodes)"));
  }

  // NOTE: Caller must hold collection_mutex_
  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(collection_id)));
  }

  // Assign shards in round-robin fashion
  std::vector<core::ShardId> shard_ids;
  for (size_t i = 0; i < num_shards; ++i) {
    core::ShardId shard_id = shard_manager_->AssignShard(core::MakeVectorId(i));
    shard_ids.push_back(shard_id);

    // Set primary node
    core::NodeId primary = data_nodes[i % data_nodes.size()].node_id;
    shard_manager_->SetPrimaryNode(shard_id, primary);

    // Set replica nodes
    for (size_t r = 1; r < replication_factor; ++r) {
      core::NodeId replica = data_nodes[(i + r) % data_nodes.size()].node_id;
      if (replica != primary) {
        shard_manager_->AddReplica(shard_id, replica);
      }
    }
  }

  it->second.shard_ids = shard_ids;

  return absl::OkStatus();
}

bool Coordinator::IsHealthy() const {
  return GetHealthyNodeCount() > 0;
}

size_t Coordinator::GetHealthyNodeCount() const {
  std::shared_lock lock(node_mutex_);
  size_t count = 0;

  for (const auto& [node_id, info] : nodes_) {
    if (info.status == NodeStatus::HEALTHY) {
      ++count;
    }
  }

  return count;
}

float Coordinator::GetClusterLoad() const {
  std::shared_lock lock(node_mutex_);
  if (nodes_.empty()) {
    return 0.0f;
  }

  float total_cpu = 0.0f;
  for (const auto& [node_id, info] : nodes_) {
    total_cpu += info.cpu_usage_percent;
  }

  return total_cpu / nodes_.size();
}

void Coordinator::StartHealthCheckLoop() {
  health_check_thread_ = std::make_unique<std::thread>([this] { HealthCheckLoop(); });
  utils::Logger::Instance().Info("Health check loop started");
}

void Coordinator::StopHealthCheckLoop() {
  if (health_check_thread_ && health_check_thread_->joinable()) {
    health_check_thread_->join();
    utils::Logger::Instance().Info("Health check loop stopped");
  }
}

void Coordinator::HealthCheckLoop() {
  while (running_.load(std::memory_order_acquire)) {
    DetectFailedNodes();
    std::this_thread::sleep_for(std::chrono::milliseconds(kHealthCheckIntervalMs));
  }
}

void Coordinator::DetectFailedNodes() {
  uint64_t current_ts = GetCurrentTimestamp();
  std::vector<core::NodeId> failed_nodes;

  {
    std::shared_lock lock(node_mutex_);
    for (const auto& [node_id, info] : nodes_) {
      if (info.status == NodeStatus::HEALTHY &&
          (current_ts - info.last_heartbeat_ts) > kNodeTimeoutMs) {
        failed_nodes.push_back(node_id);
      }
    }
  }

  for (core::NodeId node_id : failed_nodes) {
    utils::Logger::Instance().Warn("Node {} failed (heartbeat timeout)", core::ToUInt32(node_id));
    UpdateNodeStatus(node_id, NodeStatus::FAILED);

    // TODO: Trigger failover and recovery
  }
}

}  // namespace cluster
}  // namespace gvdb

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/coordinator.h"
#include "cluster/internal_client.h"
#include "utils/logger.h"
#include "network/proto_conversions.h"
#include "absl/strings/str_cat.h"
#include "internal.grpc.pb.h"
#include <chrono>
#include <thread>
#include <set>

namespace gvdb {
namespace cluster {

namespace {
constexpr uint64_t kHealthCheckIntervalMs = 5000;  // 5 seconds
constexpr uint64_t kNodeTimeoutMs = 30000;         // 30 seconds

// Convert proto::internal::NodeInfo to Coordinator::NodeInfo
NodeInfo ConvertProtoNodeInfo(const proto::internal::NodeInfo& proto_info) {
  NodeInfo info;
  info.node_id = core::MakeNodeId(proto_info.node_id());

  // Convert node type
  switch (proto_info.node_type()) {
    case proto::internal::NodeType::NODE_TYPE_COORDINATOR:
      info.type = NodeType::COORDINATOR;
      break;
    case proto::internal::NodeType::NODE_TYPE_DATA_NODE:
      info.type = NodeType::DATA_NODE;
      break;
    case proto::internal::NodeType::NODE_TYPE_QUERY_NODE:
      info.type = NodeType::QUERY_NODE;
      break;
    case proto::internal::NodeType::NODE_TYPE_PROXY:
      info.type = NodeType::PROXY;
      break;
    default:
      info.type = NodeType::QUERY_NODE;  // Default
      break;
  }

  // Convert status - map proto READY to HEALTHY
  switch (proto_info.status()) {
    case proto::internal::NodeStatus::NODE_STATUS_STARTING:
      info.status = NodeStatus::STARTING;
      break;
    case proto::internal::NodeStatus::NODE_STATUS_READY:
      info.status = NodeStatus::HEALTHY;  // Map READY to HEALTHY
      break;
    case proto::internal::NodeStatus::NODE_STATUS_DEGRADED:
      info.status = NodeStatus::DEGRADED;
      break;
    case proto::internal::NodeStatus::NODE_STATUS_DOWN:
      info.status = NodeStatus::FAILED;  // Map DOWN to FAILED
      break;
    default:
      info.status = NodeStatus::HEALTHY;  // Default
      break;
  }

  info.address = proto_info.grpc_address();
  info.memory_capacity_bytes = proto_info.memory_total_bytes();
  info.memory_used_bytes = proto_info.memory_used_bytes();
  info.disk_capacity_bytes = proto_info.disk_total_bytes();
  info.disk_used_bytes = proto_info.disk_used_bytes();
  info.cpu_usage_percent = 0.0f;  // Not in proto, leave at 0
  info.last_heartbeat_ts = 0;  // Not available from proto, handled by NodeRegistry

  return info;
}

// Convert RegisteredNode to Coordinator::NodeInfo
NodeInfo ConvertRegisteredNode(const RegisteredNode& reg_node) {
  return ConvertProtoNodeInfo(reg_node.info);
}

// Convert Coordinator::NodeInfo to proto::internal::NodeInfo
proto::internal::NodeInfo ToProtoNodeInfo(const NodeInfo& info) {
  proto::internal::NodeInfo proto_info;
  proto_info.set_node_id(core::ToUInt32(info.node_id));

  switch (info.type) {
    case NodeType::COORDINATOR:
      proto_info.set_node_type(proto::internal::NodeType::NODE_TYPE_COORDINATOR);
      break;
    case NodeType::DATA_NODE:
      proto_info.set_node_type(proto::internal::NodeType::NODE_TYPE_DATA_NODE);
      break;
    case NodeType::QUERY_NODE:
      proto_info.set_node_type(proto::internal::NodeType::NODE_TYPE_QUERY_NODE);
      break;
    case NodeType::PROXY:
      proto_info.set_node_type(proto::internal::NodeType::NODE_TYPE_PROXY);
      break;
  }

  switch (info.status) {
    case NodeStatus::STARTING:
      proto_info.set_status(proto::internal::NodeStatus::NODE_STATUS_STARTING);
      break;
    case NodeStatus::HEALTHY:
      proto_info.set_status(proto::internal::NodeStatus::NODE_STATUS_READY);
      break;
    case NodeStatus::DEGRADED:
      proto_info.set_status(proto::internal::NodeStatus::NODE_STATUS_DEGRADED);
      break;
    case NodeStatus::FAILED:
      proto_info.set_status(proto::internal::NodeStatus::NODE_STATUS_DOWN);
      break;
    case NodeStatus::LEAVING:
      proto_info.set_status(proto::internal::NodeStatus::NODE_STATUS_DOWN);
      break;
  }

  proto_info.set_grpc_address(info.address);
  proto_info.set_memory_total_bytes(info.memory_capacity_bytes);
  proto_info.set_memory_used_bytes(info.memory_used_bytes);
  proto_info.set_disk_total_bytes(info.disk_capacity_bytes);
  proto_info.set_disk_used_bytes(info.disk_used_bytes);

  return proto_info;
}

}  // anonymous namespace

Coordinator::Coordinator(
    std::shared_ptr<ShardManager> shard_manager,
    std::shared_ptr<NodeRegistry> node_registry,
    std::shared_ptr<IInternalServiceClientFactory> client_factory)
    : shard_manager_(std::move(shard_manager)),
      node_registry_(std::move(node_registry)),
      client_factory_(std::move(client_factory)) {
  utils::Logger::Instance().Info("Coordinator initialized (using NodeRegistry, distributed_mode={})",
                                  client_factory_ != nullptr);
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
  // Check if node already exists in NodeRegistry
  RegisteredNode existing;
  if (node_registry_->GetNode(core::ToUInt32(node_info.node_id), &existing)) {
    return absl::AlreadyExistsError(
        absl::StrCat("Node already registered: ", core::ToUInt32(node_info.node_id)));
  }

  // Register with shard manager first (can fail)
  auto status = shard_manager_->RegisterNode(node_info.node_id);
  if (!status.ok()) {
    return status;
  }

  // Register with NodeRegistry (updates heartbeat timestamp automatically)
  node_registry_->UpdateNode(ToProtoNodeInfo(node_info));

  utils::Logger::Instance().Info("Registered node {} (type: {}, address: {})",
                                 core::ToUInt32(node_info.node_id),
                                 static_cast<int>(node_info.type),
                                 node_info.address);

  return absl::OkStatus();
}

absl::Status Coordinator::UnregisterNode(core::NodeId node_id) {
  // Check if node exists in NodeRegistry
  RegisteredNode existing;
  if (!node_registry_->GetNode(core::ToUInt32(node_id), &existing)) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  // Remove from NodeRegistry
  node_registry_->RemoveNode(core::ToUInt32(node_id));

  // Unregister from shard manager
  shard_manager_->UnregisterNode(node_id, false);

  utils::Logger::Instance().Info("Unregistered node {}", core::ToUInt32(node_id));

  return absl::OkStatus();
}

absl::Status Coordinator::UpdateNodeStatus(core::NodeId node_id, NodeStatus status) {
  // Get current node info from registry
  RegisteredNode existing;
  if (!node_registry_->GetNode(core::ToUInt32(node_id), &existing)) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  // Convert current info and update status
  NodeInfo info = ConvertRegisteredNode(existing);
  info.status = status;
  node_registry_->UpdateNode(ToProtoNodeInfo(info));

  utils::Logger::Instance().Info("Updated node {} status to {}",
                                 core::ToUInt32(node_id),
                                 static_cast<int>(status));

  return absl::OkStatus();
}

absl::Status Coordinator::ProcessHeartbeat(core::NodeId node_id, const NodeInfo& node_info) {
  // Delegate to NodeRegistry — UpdateNode refreshes the heartbeat timestamp
  node_registry_->UpdateNode(ToProtoNodeInfo(node_info));
  return absl::OkStatus();
}

absl::StatusOr<NodeInfo> Coordinator::GetNodeInfo(core::NodeId node_id) const {
  RegisteredNode reg_node;
  if (!node_registry_->GetNode(core::ToUInt32(node_id), &reg_node)) {
    return absl::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  return ConvertRegisteredNode(reg_node);
}

std::vector<NodeInfo> Coordinator::GetAllNodes() const {
  // Get all nodes from NodeRegistry (includes both healthy and failed)
  auto registered_nodes = node_registry_->GetAllNodes();

  // Convert to Coordinator::NodeInfo
  std::vector<NodeInfo> result;
  result.reserve(registered_nodes.size());

  for (const auto& reg_node : registered_nodes) {
    result.push_back(ConvertRegisteredNode(reg_node));
  }

  return result;
}

std::vector<NodeInfo> Coordinator::GetHealthyNodes(NodeType type) const {
  // Convert Coordinator::NodeType to proto::internal::NodeType
  proto::internal::NodeType proto_type;
  switch (type) {
    case NodeType::COORDINATOR:
      proto_type = proto::internal::NodeType::NODE_TYPE_COORDINATOR;
      break;
    case NodeType::DATA_NODE:
      proto_type = proto::internal::NodeType::NODE_TYPE_DATA_NODE;
      break;
    case NodeType::QUERY_NODE:
      proto_type = proto::internal::NodeType::NODE_TYPE_QUERY_NODE;
      break;
    case NodeType::PROXY:
      proto_type = proto::internal::NodeType::NODE_TYPE_PROXY;
      break;
    default:
      proto_type = proto::internal::NodeType::NODE_TYPE_QUERY_NODE;
      break;
  }

  // Get healthy nodes of the specified type from NodeRegistry
  auto registered_nodes = node_registry_->GetHealthyNodesByType(proto_type);

  // Convert to Coordinator::NodeInfo
  std::vector<NodeInfo> result;
  result.reserve(registered_nodes.size());

  for (const auto& reg_node : registered_nodes) {
    result.push_back(ConvertRegisteredNode(reg_node));
  }

  utils::Logger::Instance().Debug("GetHealthyNodes(type={}) returned {} nodes",
                                   static_cast<int>(type), result.size());

  return result;
}

absl::StatusOr<core::CollectionId> Coordinator::CreateCollection(
    const std::string& name,
    core::Dimension dimension,
    core::MetricType metric_type,
    core::IndexType index_type,
    size_t replication_factor,
    size_t num_shards) {

  std::unique_lock lock(collection_mutex_);

  if (num_shards == 0) num_shards = 1;

  if (collection_name_to_id_.count(name) > 0) {
    return absl::AlreadyExistsError(absl::StrCat("Collection already exists: ", name));
  }

  core::CollectionId collection_id = AllocateCollectionId();

  CollectionMetadata metadata;
  metadata.collection_id = collection_id;
  metadata.collection_name = name;
  metadata.dimension = dimension;
  metadata.metric_type = metric_type;
  metadata.index_type = index_type;
  metadata.num_shards = num_shards;
  metadata.replication_factor = replication_factor;
  metadata.created_at = GetCurrentTimestamp();
  metadata.updated_at = metadata.created_at;

  collections_[collection_id] = metadata;
  collection_name_to_id_[name] = collection_id;

  utils::Logger::Instance().Info(
      "Created collection '{}' ID={} (dim={}, shards={}, replication={})",
      name, core::ToUInt32(collection_id), dimension, num_shards, replication_factor);

  // Assign shards to nodes
  auto assign_status = AssignShardsToCollection(collection_id, num_shards, replication_factor);
  if (!assign_status.ok()) {
    collections_.erase(collection_id);
    collection_name_to_id_.erase(name);
    return assign_status;
  }

  std::string metric_str = network::toString(metric_type);
  std::string index_str = network::toString(index_type);

  if (!client_factory_) {
    utils::Logger::Instance().Info(
        "Collection '{}' created (distributed mode disabled)", name);
    return collection_id;
  }

  // Create one segment per shard on its primary node
  const auto& shard_ids = collections_[collection_id].shard_ids;

  for (uint32_t i = 0; i < shard_ids.size(); ++i) {
    core::ShardId shard_id = shard_ids[i];
    core::SegmentId seg_id = ShardSegmentId(collection_id, i);

    auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
    if (!primary_result.ok() || *primary_result == core::kInvalidNodeId) {
      utils::Logger::Instance().Warn("No primary node for shard {}, skipping segment",
                                      core::ToUInt16(shard_id));
      continue;
    }

    auto* client = GetOrCreateDataNodeClient(*primary_result);
    if (!client) continue;

    proto::internal::CreateSegmentRequest request;
    request.set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
    request.set_collection_id(core::ToUInt32(collection_id));
    request.set_dimension(dimension);
    request.set_metric_type(metric_str);
    request.set_index_type(index_str);

    proto::internal::CreateSegmentResponse response;
    grpc::ClientContext context;

    grpc::Status status = client->CreateSegment(&context, request, &response);
    if (status.ok() && response.success()) {
      utils::Logger::Instance().Info("Created segment {} (shard {}) on node {}",
                                      core::ToUInt32(seg_id), i,
                                      core::ToUInt32(*primary_result));
    } else {
      std::string err = status.ok() ? response.message() : status.error_message();
      utils::Logger::Instance().Error("Failed to create segment {} on node {}: {}",
                                       core::ToUInt32(seg_id),
                                       core::ToUInt32(*primary_result), err);
    }
  }

  utils::Logger::Instance().Info(
      "Collection '{}' created with {} shards", name, num_shards);

  return collection_id;
}

absl::Status Coordinator::DropCollection(const std::string& name) {
  std::unique_lock lock(collection_mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return absl::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  core::CollectionId collection_id = name_it->second;

  // Get collection metadata before erasing
  auto coll_it = collections_.find(collection_id);
  if (coll_it == collections_.end()) {
    return absl::InternalError("Collection metadata inconsistency");
  }

  CollectionMetadata metadata = coll_it->second;  // Copy metadata

  // Distributed cleanup (only if factory is provided)
  if (client_factory_) {
    utils::Logger::Instance().Info("Starting distributed cleanup for collection '{}' (ID: {})",
                                    name, core::ToUInt32(collection_id));

    // Delete all shard segments from their respective nodes
    for (uint32_t i = 0; i < metadata.shard_ids.size(); ++i) {
      core::ShardId shard_id = metadata.shard_ids[i];
      core::SegmentId seg_id = ShardSegmentId(collection_id, i);

      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (primary_result.ok() && *primary_result != core::kInvalidNodeId) {
        auto* client = GetOrCreateDataNodeClient(*primary_result);
        if (client) {
          proto::internal::DeleteSegmentRequest del_req;
          del_req.set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
          del_req.set_force(true);
          proto::internal::DeleteSegmentResponse del_resp;
          grpc::ClientContext del_ctx;
          client->DeleteSegment(&del_ctx, del_req, &del_resp);
        }
      }
    }

    // Unassign shards from shard manager
    for (core::ShardId shard_id : metadata.shard_ids) {
      // Clear primary assignment
      auto status = shard_manager_->SetPrimaryNode(shard_id, core::kInvalidNodeId);
      if (!status.ok()) {
        utils::Logger::Instance().Warn("Failed to clear primary for shard {}: {}",
                                        core::ToUInt16(shard_id), status.message());
      }

      // Remove all replicas
      auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
      if (replicas_result.ok()) {
        for (core::NodeId replica : replicas_result.value()) {
          if (replica != core::kInvalidNodeId) {
            auto rm_status = shard_manager_->RemoveReplica(shard_id, replica);
            if (!rm_status.ok()) {
              utils::Logger::Instance().Warn("Failed to remove replica {} from shard {}: {}",
                                              core::ToUInt32(replica), core::ToUInt16(shard_id),
                                              rm_status.message());
            }
          }
        }
      }
    }
  }

  // Remove from coordinator metadata
  collections_.erase(collection_id);
  collection_name_to_id_.erase(name);

  utils::Logger::Instance().Info("Dropped collection '{}' (ID: {})",
                                  name, core::ToUInt32(collection_id));

  return absl::OkStatus();
}

absl::Status Coordinator::DropCollection(core::CollectionId collection_id) {
  std::shared_lock lock(collection_mutex_);

  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return absl::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(collection_id)));
  }

  std::string name = it->second.collection_name;
  lock.unlock();  // Release lock before calling the other overload

  // Delegate to the by-name implementation which handles distributed cleanup
  return DropCollection(name);
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
    auto status = shard_manager_->SetPrimaryNode(shard_id, primary);
    if (!status.ok()) {
      return status;
    }

    // Set replica nodes
    for (size_t r = 1; r < replication_factor; ++r) {
      core::NodeId replica = data_nodes[(i + r) % data_nodes.size()].node_id;
      if (replica != primary) {
        auto replica_status = shard_manager_->AddReplica(shard_id, replica);
        if (!replica_status.ok()) {
          return replica_status;
        }
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
  // Get all healthy nodes from NodeRegistry
  auto healthy_nodes = node_registry_->GetHealthyNodes();
  return healthy_nodes.size();
}

float Coordinator::GetClusterLoad() const {
  auto all_nodes = node_registry_->GetAllNodes();
  if (all_nodes.empty()) {
    return 0.0f;
  }

  float total_cpu = 0.0f;
  for (const auto& reg_node : all_nodes) {
    // cpu_usage_percent is not in proto, so this will be 0
    // TODO: Add CPU usage to proto if needed
    total_cpu += 0.0f;
  }

  return total_cpu / all_nodes.size();
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
    CheckReplication();
    std::this_thread::sleep_for(std::chrono::milliseconds(kHealthCheckIntervalMs));
  }
}

void Coordinator::CheckReplication() {
  if (!client_factory_) return;

  std::shared_lock lock(collection_mutex_);
  for (const auto& [coll_id, metadata] : collections_) {
    if (metadata.replication_factor <= 1) continue;

    for (uint32_t i = 0; i < metadata.shard_ids.size(); ++i) {
      core::ShardId shard_id = metadata.shard_ids[i];

      auto primary_result = shard_manager_->GetPrimaryNode(shard_id);
      if (!primary_result.ok() || *primary_result == core::kInvalidNodeId) continue;

      auto replicas_result = shard_manager_->GetReplicaNodes(shard_id);
      size_t actual_replicas = 1;  // Primary counts as 1
      if (replicas_result.ok()) {
        actual_replicas += replicas_result->size();
      }

      if (actual_replicas < metadata.replication_factor) {
        // Under-replicated — find a healthy node to replicate to
        auto healthy_nodes = GetHealthyNodes(NodeType::DATA_NODE);
        for (const auto& node : healthy_nodes) {
          if (node.node_id == *primary_result) continue;
          bool already_replica = false;
          if (replicas_result.ok()) {
            for (const auto& r : *replicas_result) {
              if (r == node.node_id) { already_replica = true; break; }
            }
          }
          if (already_replica) continue;

          // Found a node that doesn't have this shard — replicate
          core::SegmentId seg_id = ShardSegmentId(coll_id, i);
          lock.unlock();  // Release lock during RPC
          auto status = ReplicateSegmentData(seg_id, *primary_result, node.node_id);
          lock.lock();    // Re-acquire (collections_ may have changed, but that's OK for a best-effort check)

          if (status.ok()) {
            (void)shard_manager_->AddReplica(shard_id, node.node_id);
            utils::Logger::Instance().Info(
                "Auto-replicated shard {} (segment {}) to node {}",
                core::ToUInt16(shard_id), core::ToUInt32(seg_id),
                core::ToUInt32(node.node_id));
          }
          break;  // One replication per check cycle per shard
        }
      }
    }
  }
}

void Coordinator::DetectFailedNodes() {
  auto failed_ids = node_registry_->DetectFailedNodes();

  for (uint32_t node_id : failed_ids) {
    utils::Logger::Instance().Warn("Node {} failed (heartbeat timeout)", node_id);
    HandleFailedNode(core::MakeNodeId(node_id));
  }
}

void Coordinator::HandleFailedNode(core::NodeId failed_node_id) {
  // Get all shards assigned to the failed node
  auto shards = shard_manager_->GetShardsForNode(failed_node_id);

  for (const auto& shard_info : shards) {
    auto primary_result = shard_manager_->GetPrimaryNode(shard_info.shard_id);
    if (!primary_result.ok()) continue;

    if (*primary_result == failed_node_id) {
      // This node was primary — try to promote a replica
      auto replicas_result = shard_manager_->GetReplicaNodes(shard_info.shard_id);
      if (!replicas_result.ok()) continue;
      const auto& replicas = *replicas_result;

      core::NodeId new_primary = core::kInvalidNodeId;
      for (const auto& replica : replicas) {
        if (replica != failed_node_id) {
          // Check if replica is healthy
          RegisteredNode node;
          if (node_registry_->GetNode(core::ToUInt32(replica), &node) &&
              node.info.status() == proto::internal::NodeStatus::NODE_STATUS_READY) {
            new_primary = replica;
            break;
          }
        }
      }

      if (new_primary != core::kInvalidNodeId) {
        auto status = shard_manager_->SetPrimaryNode(shard_info.shard_id, new_primary);
        if (status.ok()) {
          utils::Logger::Instance().Info(
              "Promoted node {} to primary for shard {} (was on failed node {})",
              core::ToUInt32(new_primary), core::ToUInt16(shard_info.shard_id),
              core::ToUInt32(failed_node_id));
        }
      } else {
        utils::Logger::Instance().Error(
            "No healthy replica available for shard {} (primary node {} failed)",
            core::ToUInt16(shard_info.shard_id), core::ToUInt32(failed_node_id));
      }
    }
  }

  // Remove failed node from shard manager
  shard_manager_->UnregisterNode(failed_node_id, false);
  utils::Logger::Instance().Info("Removed failed node {} from shard manager",
                                  core::ToUInt32(failed_node_id));
}

absl::Status Coordinator::ReplicateSegmentData(
    core::SegmentId segment_id,
    core::NodeId source_node,
    core::NodeId target_node) {

  if (!client_factory_) {
    return absl::FailedPreconditionError("Distributed mode not enabled");
  }

  // Step 1: Get segment from source node
  auto* source_client = GetOrCreateDataNodeClient(source_node);
  if (!source_client) {
    return absl::UnavailableError(
        absl::StrCat("Cannot reach source node ", core::ToUInt32(source_node)));
  }

  proto::internal::GetSegmentRequest get_req;
  get_req.set_segment_id(static_cast<uint64_t>(core::ToUInt32(segment_id)));
  proto::internal::GetSegmentResponse get_resp;
  grpc::ClientContext get_ctx;

  auto get_status = source_client->GetSegment(&get_ctx, get_req, &get_resp);
  if (!get_status.ok()) {
    return absl::InternalError(
        absl::StrCat("GetSegment from node ", core::ToUInt32(source_node),
                     " failed: ", get_status.error_message()));
  }

  if (get_resp.segment_data().empty()) {
    return absl::NotFoundError("Segment data is empty on source node");
  }

  // Step 2: Send segment to target node
  auto* target_client = GetOrCreateDataNodeClient(target_node);
  if (!target_client) {
    return absl::UnavailableError(
        absl::StrCat("Cannot reach target node ", core::ToUInt32(target_node)));
  }

  proto::internal::ReplicateSegmentRequest rep_req;
  auto* seg_info = rep_req.mutable_segment_info();
  if (get_resp.has_segment_info()) {
    *seg_info = get_resp.segment_info();
  }
  rep_req.set_segment_data(get_resp.segment_data());

  proto::internal::ReplicateSegmentResponse rep_resp;
  grpc::ClientContext rep_ctx;

  auto rep_status = target_client->ReplicateSegment(&rep_ctx, rep_req, &rep_resp);
  if (!rep_status.ok()) {
    return absl::InternalError(
        absl::StrCat("ReplicateSegment to node ", core::ToUInt32(target_node),
                     " failed: ", rep_status.error_message()));
  }

  if (!rep_resp.success()) {
    return absl::InternalError(rep_resp.message());
  }

  utils::Logger::Instance().Info("Replicated segment {} from node {} to node {}",
                                  core::ToUInt32(segment_id),
                                  core::ToUInt32(source_node),
                                  core::ToUInt32(target_node));
  return absl::OkStatus();
}

IInternalServiceClient* Coordinator::GetOrCreateDataNodeClient(core::NodeId node_id) {
  // Check if factory is available
  if (!client_factory_) {
    return nullptr;  // Distributed mode not enabled
  }

  // Try read lock first (fast path for existing clients)
  {
    std::shared_lock lock(client_mutex_);
    auto it = data_node_clients_.find(node_id);
    if (it != data_node_clients_.end()) {
      return it->second.get();
    }
  }

  // Need to create new client - upgrade to write lock
  std::unique_lock lock(client_mutex_);

  // Double-check in case another thread created it while we waited
  auto it = data_node_clients_.find(node_id);
  if (it != data_node_clients_.end()) {
    return it->second.get();
  }

  // Get node info from NodeRegistry
  RegisteredNode node;
  if (!node_registry_->GetNode(core::ToUInt32(node_id), &node)) {
    utils::Logger::Instance().Error("Cannot create client for unknown node {}", core::ToUInt32(node_id));
    return nullptr;
  }

  const std::string& address = node.info.grpc_address();

  // Use factory to create client
  auto client = client_factory_->CreateClient(node_id, address);
  if (!client) {
    utils::Logger::Instance().Error("Factory failed to create client for node {}", core::ToUInt32(node_id));
    return nullptr;
  }

  // Store in map first (transfers ownership)
  data_node_clients_[node_id] = std::move(client);

  // Then return pointer from map
  return data_node_clients_[node_id].get();
}

}  // namespace cluster
}  // namespace gvdb
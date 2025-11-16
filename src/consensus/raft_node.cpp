#include "consensus/raft_node.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace consensus {

RaftNode::RaftNode(const RaftConfig& config)
    : config_(config) {

  if (!config_.IsValid()) {
    throw std::invalid_argument("Invalid Raft configuration");
  }

  utils::Logger::Instance().Info("RaftNode created (node_id={}, single_node_mode={})",
                                 config_.node_id,
                                 config_.single_node_mode);
}

RaftNode::~RaftNode() {
  if (running_.load(std::memory_order_acquire)) {
    Shutdown();
  }
}

core::Status RaftNode::Start() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (running_.load(std::memory_order_acquire)) {
    return core::FailedPreconditionError("RaftNode already running");
  }

  utils::Logger::Instance().Info("Starting RaftNode (node_id={})", config_.node_id);

  // In single-node mode, immediately become leader
  if (config_.single_node_mode) {
    is_leader_.store(true, std::memory_order_release);
    utils::Logger::Instance().Info("RaftNode is leader (single-node mode)");
  } else {
    // TODO: In multi-node mode, start Raft protocol
    // For now, we only support single-node
    return core::UnimplementedError(
        "Multi-node Raft is not yet implemented. Use single_node_mode=true.");
  }

  running_.store(true, std::memory_order_release);

  utils::Logger::Instance().Info("RaftNode started successfully");
  return core::OkStatus();
}

core::Status RaftNode::Shutdown() {
  std::lock_guard<std::mutex> lock(mutex_);

  if (!running_.load(std::memory_order_acquire)) {
    return core::OkStatus();  // Already stopped
  }

  utils::Logger::Instance().Info("Shutting down RaftNode");

  is_leader_.store(false, std::memory_order_release);
  running_.store(false, std::memory_order_release);

  utils::Logger::Instance().Info("RaftNode shut down successfully");
  return core::OkStatus();
}

bool RaftNode::IsRunning() const {
  return running_.load(std::memory_order_acquire);
}

bool RaftNode::IsLeader() const {
  return is_leader_.load(std::memory_order_acquire);
}

int RaftNode::GetLeaderId() const {
  if (is_leader_.load(std::memory_order_acquire)) {
    return config_.node_id;
  }
  return -1;  // No leader
}

core::StatusOr<core::CollectionId> RaftNode::CreateCollection(
    const std::string& name,
    core::Dimension dimension,
    core::MetricType metric_type,
    core::IndexType index_type,
    size_t replication_factor) {

  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  // Get timestamp from TSO
  core::Timestamp ts = tso_.GetTimestamp();

  // Apply directly to metadata store (in single-node mode, no consensus needed)
  auto result = metadata_store_.CreateCollection(
      name, dimension, metric_type, index_type, replication_factor, ts);

  if (result.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return result;
}

core::Status RaftNode::DropCollection(core::CollectionId collection_id) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.DropCollection(collection_id, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::StatusOr<cluster::CollectionMetadata> RaftNode::GetCollectionMetadata(
    core::CollectionId id) const {
  return metadata_store_.GetCollectionMetadata(id);
}

core::StatusOr<cluster::CollectionMetadata> RaftNode::GetCollectionMetadata(
    const std::string& name) const {
  return metadata_store_.GetCollectionMetadata(name);
}

std::vector<cluster::CollectionMetadata> RaftNode::ListCollections() const {
  return metadata_store_.ListCollections();
}

core::Status RaftNode::RegisterNode(const cluster::NodeInfo& node_info) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.RegisterNode(node_info, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::Status RaftNode::UnregisterNode(core::NodeId node_id) {
  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader, cannot propose operations");
  }

  core::Timestamp ts = tso_.GetTimestamp();

  auto status = metadata_store_.UnregisterNode(node_id, ts);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

core::StatusOr<cluster::NodeInfo> RaftNode::GetNodeInfo(core::NodeId id) const {
  return metadata_store_.GetNodeInfo(id);
}

std::vector<cluster::NodeInfo> RaftNode::ListNodes() const {
  return metadata_store_.ListNodes();
}

size_t RaftNode::GetCommittedOpCount() const {
  return committed_ops_.load(std::memory_order_relaxed);
}

core::Status RaftNode::ProposeOperation(const MetadataOp& op) {
  // In single-node mode, operations are applied immediately
  // In multi-node mode, this would propose to Raft and wait for commit

  if (!IsLeader()) {
    return core::FailedPreconditionError("Not leader");
  }

  auto status = metadata_store_.Apply(op);

  if (status.ok()) {
    committed_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  return status;
}

} // namespace consensus
} // namespace gvdb

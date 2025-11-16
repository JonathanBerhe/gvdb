#pragma once

#include "consensus/raft_config.h"
#include "consensus/metadata_store.h"
#include "consensus/timestamp_oracle.h"
#include "core/status.h"
#include <atomic>
#include <memory>
#include <mutex>

namespace gvdb {
namespace consensus {

// Simplified Raft node for consensus
// In Phase 1: Operates in single-node mode (always leader)
// Future: Can be upgraded to use braft for multi-node Raft
//
// Thread-safe: All public methods are thread-safe
class RaftNode {
 public:
  explicit RaftNode(const RaftConfig& config);
  ~RaftNode();

  // Disable copy and move
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  // Lifecycle
  core::Status Start();
  core::Status Shutdown();
  bool IsRunning() const;

  // Leadership (in single-node mode, always true after Start())
  bool IsLeader() const;
  int GetLeaderId() const;
  int GetNodeId() const { return config_.node_id; }

  // Metadata operations (go through consensus)
  core::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t replication_factor);

  core::Status DropCollection(core::CollectionId collection_id);

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      core::CollectionId id) const;

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      const std::string& name) const;

  std::vector<cluster::CollectionMetadata> ListCollections() const;

  // Node operations
  core::Status RegisterNode(const cluster::NodeInfo& node_info);
  core::Status UnregisterNode(core::NodeId node_id);

  core::StatusOr<cluster::NodeInfo> GetNodeInfo(core::NodeId id) const;
  std::vector<cluster::NodeInfo> ListNodes() const;

  // Timestamp oracle access
  TimestampOracle* GetTimestampOracle() { return &tso_; }
  const TimestampOracle* GetTimestampOracle() const { return &tso_; }

  // Metadata store access (read-only)
  const MetadataStore* GetMetadataStore() const { return &metadata_store_; }

  // Statistics
  size_t GetCommittedOpCount() const;

 private:
  RaftConfig config_;
  std::atomic<bool> running_{false};
  std::atomic<bool> is_leader_{false};

  // Core components
  TimestampOracle tso_;
  MetadataStore metadata_store_;

  // Synchronization
  mutable std::mutex mutex_;

  // Statistics
  std::atomic<size_t> committed_ops_{0};

  // Helpers
  core::Status ProposeOperation(const MetadataOp& op);
};

} // namespace consensus
} // namespace gvdb

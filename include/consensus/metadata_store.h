#pragma once

#include "core/types.h"
#include "core/status.h"
#include "cluster/coordinator.h"  // For CollectionMetadata
#include <map>
#include <shared_mutex>
#include <vector>

namespace gvdb {
namespace consensus {

// Operation types that can be applied to the metadata store
enum class OpType : uint8_t {
  CREATE_COLLECTION = 1,
  DROP_COLLECTION = 2,
  REGISTER_NODE = 3,
  UNREGISTER_NODE = 4,
  UPDATE_SHARD_ASSIGNMENT = 5
};

// Metadata operation (serializable to Raft log)
struct MetadataOp {
  OpType type;
  core::Timestamp timestamp;
  std::vector<uint8_t> data;  // Serialized operation data

  MetadataOp() : type(OpType::CREATE_COLLECTION), timestamp(0) {}
  MetadataOp(OpType t, core::Timestamp ts, const std::vector<uint8_t>& d)
      : type(t), timestamp(ts), data(d) {}
};

// Deterministic metadata state machine
// Stores all cluster metadata and applies operations in total order
//
// Thread-safe: All methods use shared_mutex for concurrent read access
class MetadataStore {
 public:
  MetadataStore();
  ~MetadataStore() = default;

  // Disable copy and move
  MetadataStore(const MetadataStore&) = delete;
  MetadataStore& operator=(const MetadataStore&) = delete;

  // Apply an operation to the state machine
  // This is the core of the consensus - all operations go through here
  core::Status Apply(const MetadataOp& op);

  // Collection operations
  core::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t replication_factor,
      core::Timestamp ts);

  core::Status DropCollection(core::CollectionId collection_id, core::Timestamp ts);

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      core::CollectionId id) const;

  core::StatusOr<cluster::CollectionMetadata> GetCollectionMetadata(
      const std::string& name) const;

  std::vector<cluster::CollectionMetadata> ListCollections() const;

  // Node operations
  core::Status RegisterNode(const cluster::NodeInfo& node_info, core::Timestamp ts);
  core::Status UnregisterNode(core::NodeId node_id, core::Timestamp ts);

  core::StatusOr<cluster::NodeInfo> GetNodeInfo(core::NodeId id) const;
  std::vector<cluster::NodeInfo> ListNodes() const;

  // Snapshot support
  size_t GetVersion() const;  // Returns number of operations applied

  // Statistics
  size_t GetCollectionCount() const;
  size_t GetNodeCount() const;

 private:
  mutable std::shared_mutex mutex_;

  // State
  std::map<core::CollectionId, cluster::CollectionMetadata> collections_;
  std::map<std::string, core::CollectionId> collection_name_to_id_;
  std::map<core::NodeId, cluster::NodeInfo> nodes_;

  // ID generation
  uint32_t next_collection_id_{1};
  uint32_t next_node_id_{1};

  // Version tracking (number of operations applied)
  size_t version_{0};

  // Helper methods
  core::CollectionId AllocateCollectionId();
  core::NodeId AllocateNodeId();
};

} // namespace consensus
} // namespace gvdb

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/metadata_store.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"
#include <mutex>

namespace gvdb {
namespace consensus {

MetadataStore::MetadataStore() {
  utils::Logger::Instance().Info("MetadataStore initialized");
}

core::Status MetadataStore::Apply(const MetadataOp& op) {
  std::unique_lock lock(mutex_);

  // Increment version for each operation
  version_++;

  // TODO: In a real implementation, we'd deserialize the operation data
  // and dispatch to the appropriate handler
  // For now, this is a placeholder

  utils::Logger::Instance().Debug("Applied operation type {} at version {}",
                                  static_cast<int>(op.type), version_);
  return core::OkStatus();
}

core::StatusOr<core::CollectionId> MetadataStore::CreateCollection(
    const std::string& name,
    core::Dimension dimension,
    core::MetricType metric_type,
    core::IndexType index_type,
    size_t replication_factor,
    core::Timestamp ts) {

  std::unique_lock lock(mutex_);

  // Check if collection already exists
  if (collection_name_to_id_.count(name) > 0) {
    return core::AlreadyExistsError(absl::StrCat("Collection already exists: ", name));
  }

  // Allocate ID
  core::CollectionId collection_id = AllocateCollectionId();

  // Create metadata
  cluster::CollectionMetadata metadata;
  metadata.collection_id = collection_id;
  metadata.collection_name = name;
  metadata.dimension = dimension;
  metadata.metric_type = metric_type;
  metadata.index_type = index_type;
  metadata.replication_factor = replication_factor;
  metadata.total_vectors = 0;
  metadata.total_size_bytes = 0;
  metadata.created_at = ts;
  metadata.updated_at = ts;

  // Store
  collections_[collection_id] = metadata;
  collection_name_to_id_[name] = collection_id;

  version_++;

  utils::Logger::Instance().Info("Created collection '{}' with ID {} (dim={}, metric={}, index={})",
                                 name,
                                 core::ToUInt32(collection_id),
                                 dimension,
                                 static_cast<int>(metric_type),
                                 static_cast<int>(index_type));

  return collection_id;
}

core::Status MetadataStore::DropCollection(core::CollectionId collection_id,
                                             core::Timestamp ts) {
  std::unique_lock lock(mutex_);

  auto it = collections_.find(collection_id);
  if (it == collections_.end()) {
    return core::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(collection_id)));
  }

  std::string name = it->second.collection_name;

  // Remove from maps
  collections_.erase(it);
  collection_name_to_id_.erase(name);

  version_++;

  utils::Logger::Instance().Info("Dropped collection '{}' (ID={})", name,
                                 core::ToUInt32(collection_id));

  return core::OkStatus();
}

core::StatusOr<cluster::CollectionMetadata> MetadataStore::GetCollectionMetadata(
    core::CollectionId id) const {
  std::shared_lock lock(mutex_);

  auto it = collections_.find(id);
  if (it == collections_.end()) {
    return core::NotFoundError(
        absl::StrCat("Collection not found: ", core::ToUInt32(id)));
  }

  return it->second;
}

core::StatusOr<cluster::CollectionMetadata> MetadataStore::GetCollectionMetadata(
    const std::string& name) const {
  std::shared_lock lock(mutex_);

  auto name_it = collection_name_to_id_.find(name);
  if (name_it == collection_name_to_id_.end()) {
    return core::NotFoundError(absl::StrCat("Collection not found: ", name));
  }

  auto it = collections_.find(name_it->second);
  if (it == collections_.end()) {
    return core::InternalError("Collection ID mapping corrupted");
  }

  return it->second;
}

std::vector<cluster::CollectionMetadata> MetadataStore::ListCollections() const {
  std::shared_lock lock(mutex_);

  std::vector<cluster::CollectionMetadata> result;
  result.reserve(collections_.size());

  for (const auto& [id, metadata] : collections_) {
    result.push_back(metadata);
  }

  return result;
}

core::Status MetadataStore::RegisterNode(const cluster::NodeInfo& node_info,
                                          core::Timestamp ts) {
  std::unique_lock lock(mutex_);

  // Check if node already exists
  if (nodes_.count(node_info.node_id) > 0) {
    return core::AlreadyExistsError(
        absl::StrCat("Node already registered: ", core::ToUInt32(node_info.node_id)));
  }

  cluster::NodeInfo info = node_info;
  info.last_heartbeat_ts = ts;

  nodes_[node_info.node_id] = info;

  version_++;

  utils::Logger::Instance().Info("Registered node {} (type={}, address={})",
                                 core::ToUInt32(node_info.node_id),
                                 static_cast<int>(node_info.type),
                                 node_info.address);

  return core::OkStatus();
}

core::Status MetadataStore::UnregisterNode(core::NodeId node_id, core::Timestamp ts) {
  std::unique_lock lock(mutex_);

  if (nodes_.erase(node_id) == 0) {
    return core::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(node_id)));
  }

  version_++;

  utils::Logger::Instance().Info("Unregistered node {}", core::ToUInt32(node_id));

  return core::OkStatus();
}

core::StatusOr<cluster::NodeInfo> MetadataStore::GetNodeInfo(core::NodeId id) const {
  std::shared_lock lock(mutex_);

  auto it = nodes_.find(id);
  if (it == nodes_.end()) {
    return core::NotFoundError(
        absl::StrCat("Node not found: ", core::ToUInt32(id)));
  }

  return it->second;
}

std::vector<cluster::NodeInfo> MetadataStore::ListNodes() const {
  std::shared_lock lock(mutex_);

  std::vector<cluster::NodeInfo> result;
  result.reserve(nodes_.size());

  for (const auto& [id, info] : nodes_) {
    result.push_back(info);
  }

  return result;
}

size_t MetadataStore::GetVersion() const {
  std::shared_lock lock(mutex_);
  return version_;
}

size_t MetadataStore::GetCollectionCount() const {
  std::shared_lock lock(mutex_);
  return collections_.size();
}

size_t MetadataStore::GetNodeCount() const {
  std::shared_lock lock(mutex_);
  return nodes_.size();
}

core::CollectionId MetadataStore::AllocateCollectionId() {
  // Caller must hold lock
  return core::MakeCollectionId(next_collection_id_++);
}

core::NodeId MetadataStore::AllocateNodeId() {
  // Caller must hold lock
  return core::MakeNodeId(next_node_id_++);
}

} // namespace consensus
} // namespace gvdb
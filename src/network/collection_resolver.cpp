// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/collection_resolver.h"
#include "network/collection_metadata_cache.h"
#include "network/proto_conversions.h"
#include "storage/segment_store.h"
#include "cluster/coordinator.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "internal.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

namespace gvdb {
namespace network {

// ============================================================================
// LocalCollectionResolver — single-node mode
// ============================================================================

// Internal metadata for single-node mode
struct LocalEntry {
  core::CollectionId id;
  std::string name;
  uint32_t dimension;
  core::MetricType metric;
  core::IndexType index_type;
  size_t num_shards = 1;
  std::vector<core::SegmentId> segment_ids;  // One per shard
};

class LocalCollectionResolver : public ICollectionResolver {
 public:
  LocalCollectionResolver(
      std::shared_ptr<storage::ISegmentStore> segment_store)
      : segment_store_(std::move(segment_store)),
        next_id_(1) {}

  absl::StatusOr<core::CollectionId> GetCollectionId(
      const std::string& name) override {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(name);
    if (it == entries_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return it->second.id;
  }

  absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t num_shards = 1) override {
    std::unique_lock lock(mutex_);
    if (entries_.count(name) > 0) {
      return absl::AlreadyExistsError("Collection already exists: " + name);
    }
    if (num_shards == 0) num_shards = 1;

    core::CollectionId id = core::CollectionId(next_id_++);

    LocalEntry entry;
    entry.id = id;
    entry.name = name;
    entry.dimension = dimension;
    entry.metric = metric_type;
    entry.index_type = index_type;
    entry.num_shards = num_shards;

    // Create one segment per shard
    for (uint32_t i = 0; i < num_shards; ++i) {
      core::SegmentId seg_id = cluster::ShardSegmentId(id, i);
      auto status = segment_store_->CreateSegmentWithId(
          seg_id, id, dimension, metric_type, index_type);
      if (!status.ok()) {
        // Rollback already-created segments
        for (const auto& created : entry.segment_ids) {
          (void)segment_store_->DropSegment(created, false);
        }
        return status;
      }
      entry.segment_ids.push_back(seg_id);
    }

    entries_[name] = entry;
    utils::MetricsRegistry::Instance().SetCollectionCount(entries_.size());
    return id;
  }

  absl::Status DropCollection(const std::string& name) override {
    std::unique_lock lock(mutex_);
    auto it = entries_.find(name);
    if (it == entries_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }

    for (const auto& seg_id : it->second.segment_ids) {
      (void)segment_store_->DropSegment(seg_id, true);
    }

    entries_.erase(it);
    utils::MetricsRegistry::Instance().SetCollectionCount(entries_.size());
    return absl::OkStatus();
  }

  std::vector<CollectionInfo> ListCollections() override {
    std::shared_lock lock(mutex_);
    std::vector<CollectionInfo> result;
    result.reserve(entries_.size());
    for (const auto& [name, entry] : entries_) {
      CollectionInfo info;
      info.collection_id = entry.id;
      info.collection_name = entry.name;
      info.dimension = entry.dimension;
      info.metric_type = entry.metric;
      info.index_type = entry.index_type;
      uint64_t total_vectors = 0;
      for (const auto& seg_id : entry.segment_ids) {
        auto* segment = segment_store_->GetSegment(seg_id);
        if (segment) total_vectors += segment->GetVectorCount();
      }
      info.vector_count = total_vectors;
      result.push_back(info);
    }
    return result;
  }

  size_t CollectionCount() override {
    std::shared_lock lock(mutex_);
    return entries_.size();
  }

  absl::StatusOr<core::SegmentId> GetSegmentId(const std::string& name) override {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(name);
    if (it == entries_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return it->second.segment_ids[0];  // First shard for backward compat
  }

  absl::StatusOr<std::vector<core::SegmentId>> GetSegmentIds(
      const std::string& name) override {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(name);
    if (it == entries_.end()) {
      return absl::NotFoundError("Collection not found: " + name);
    }
    return it->second.segment_ids;
  }

  size_t GetNumShards(const std::string& name) override {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(name);
    if (it == entries_.end()) return 1;
    return it->second.num_shards;
  }

  bool SupportsDataOps() const override { return true; }

 private:
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, LocalEntry> entries_;
  uint32_t next_id_;
};

// ============================================================================
// CoordinatorCollectionResolver — coordinator mode
// ============================================================================

class CoordinatorCollectionResolver : public ICollectionResolver {
 public:
  explicit CoordinatorCollectionResolver(
      std::shared_ptr<cluster::Coordinator> coordinator)
      : coordinator_(std::move(coordinator)) {}

  absl::StatusOr<core::CollectionId> GetCollectionId(
      const std::string& name) override {
    auto result = coordinator_->GetCollectionMetadata(name);
    if (!result.ok()) return result.status();
    return result->collection_id;
  }

  absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t num_shards = 1) override {
    return coordinator_->CreateCollection(name, dimension, metric_type, index_type, 1, num_shards);
  }

  absl::Status DropCollection(const std::string& name) override {
    return coordinator_->DropCollection(name);
  }

  std::vector<CollectionInfo> ListCollections() override {
    auto collections = coordinator_->ListCollections();
    std::vector<CollectionInfo> result;
    result.reserve(collections.size());
    for (const auto& meta : collections) {
      CollectionInfo info;
      info.collection_id = meta.collection_id;
      info.collection_name = meta.collection_name;
      info.dimension = meta.dimension;
      info.metric_type = meta.metric_type;
      info.index_type = meta.index_type;
      info.vector_count = meta.total_vectors;
      result.push_back(info);
    }
    return result;
  }

  size_t CollectionCount() override {
    return coordinator_->ListCollections().size();
  }

  absl::StatusOr<core::SegmentId> GetSegmentId(const std::string&) override {
    return absl::UnimplementedError(
        "Data operations not supported on coordinator nodes");
  }

  bool SupportsDataOps() const override { return false; }

 private:
  std::shared_ptr<cluster::Coordinator> coordinator_;
};

// ============================================================================
// CachedCoordinatorResolver — distributed data/query nodes
// ============================================================================

class CachedCoordinatorResolver : public ICollectionResolver {
 public:
  explicit CachedCoordinatorResolver(const std::string& coordinator_address)
      : cache_(std::make_unique<CollectionMetadataCache>()) {
    auto channel = grpc::CreateChannel(coordinator_address,
                                        grpc::InsecureChannelCredentials());
    stub_ = proto::internal::InternalService::NewStub(channel);
    utils::Logger::Instance().Info(
        "CachedCoordinatorResolver connected to {}", coordinator_address);
  }

  absl::StatusOr<core::CollectionId> GetCollectionId(
      const std::string& name) override {
    // Check cache first
    auto cached = cache_->GetByName(name);
    if (cached.ok()) return cached->collection_id;

    // Cache miss — pull from coordinator
    return FetchFromCoordinator(name);
  }

  absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string&, core::Dimension, core::MetricType, core::IndexType,
      size_t) override {
    return absl::UnimplementedError(
        "CreateCollection not supported on distributed data/query nodes. "
        "Send to coordinator instead.");
  }

  absl::Status DropCollection(const std::string&) override {
    return absl::UnimplementedError(
        "DropCollection not supported on distributed data/query nodes. "
        "Send to coordinator instead.");
  }

  std::vector<CollectionInfo> ListCollections() override {
    // Distributed nodes don't have a full collection list
    return {};
  }

  size_t CollectionCount() override { return cache_->Size(); }

  absl::StatusOr<core::SegmentId> GetSegmentId(const std::string& name) override {
    auto id_result = GetCollectionId(name);
    if (!id_result.ok()) return id_result.status();
    return cluster::ShardSegmentId(*id_result, 0);
  }

  absl::StatusOr<std::vector<core::SegmentId>> GetSegmentIds(
      const std::string& name) override {
    auto id_result = GetCollectionId(name);
    if (!id_result.ok()) return id_result.status();

    size_t shards = GetNumShards(name);
    std::vector<core::SegmentId> ids;
    ids.reserve(shards);
    for (size_t i = 0; i < shards; ++i) {
      ids.push_back(cluster::ShardSegmentId(*id_result, i));
    }
    return ids;
  }

  size_t GetNumShards(const std::string& name) override {
    auto cached = cache_->GetByName(name);
    if (cached.ok()) {
      return cached->num_shards > 0 ? cached->num_shards : 1;
    }
    return 1;
  }

  bool SupportsDataOps() const override { return true; }

  proto::internal::InternalService::Stub* GetCoordinatorStub() override {
    return stub_.get();
  }

  absl::StatusOr<std::vector<ShardTarget>> GetShardTargets(
      const std::string& collection_name) override {
    grpc::ClientContext context;
    proto::internal::RouteQueryRequest request;
    request.set_collection_name(collection_name);
    proto::internal::RouteQueryResponse response;

    auto status = stub_->RouteQuery(&context, request, &response);
    if (!status.ok()) {
      return fromGrpcStatus(status);
    }

    std::vector<ShardTarget> targets;
    int n = response.target_shard_ids_size();
    targets.reserve(n);
    for (int i = 0; i < n; ++i) {
      ShardTarget t;
      t.shard_id = response.target_shard_ids(i);
      t.collection_id = response.collection_id();
      t.node_address = i < response.target_node_addresses_size()
                           ? response.target_node_addresses(i) : "";
      targets.push_back(std::move(t));
    }
    return targets;
  }

 private:
  absl::StatusOr<core::CollectionId> FetchFromCoordinator(const std::string& name) {
    grpc::ClientContext context;
    proto::internal::GetCollectionMetadataRequest request;
    request.set_collection_name(name);
    proto::internal::GetCollectionMetadataResponse response;

    auto status = stub_->GetCollectionMetadata(&context, request, &response);
    if (!status.ok()) {
      return fromGrpcStatus(status);
    }

    if (!response.found()) {
      return absl::NotFoundError("Collection not found: " + name);
    }

    const auto& pm = response.metadata();
    CollectionMetadata metadata;
    metadata.collection_id = core::MakeCollectionId(pm.collection_id());
    metadata.collection_name = pm.collection_name();
    metadata.dimension = pm.dimension();

    auto metric = metricTypeFromString(pm.metric_type());
    if (!metric.ok()) return metric.status();
    metadata.metric_type = *metric;

    auto index = indexTypeFromString(pm.index_type());
    if (!index.ok()) return index.status();
    metadata.index_type = *index;

    metadata.num_shards = pm.shard_count() > 0 ? pm.shard_count() : 1;

    cache_->Put(metadata);
    return metadata.collection_id;
  }

  std::unique_ptr<CollectionMetadataCache> cache_;
  std::unique_ptr<proto::internal::InternalService::Stub> stub_;
};

// ============================================================================
// Factory functions
// ============================================================================

std::unique_ptr<ICollectionResolver> MakeLocalResolver(
    std::shared_ptr<storage::ISegmentStore> segment_store) {
  return std::make_unique<LocalCollectionResolver>(std::move(segment_store));
}

std::unique_ptr<ICollectionResolver> MakeCoordinatorResolver(
    std::shared_ptr<cluster::Coordinator> coordinator) {
  return std::make_unique<CoordinatorCollectionResolver>(std::move(coordinator));
}

std::unique_ptr<ICollectionResolver> MakeCachedCoordinatorResolver(
    const std::string& coordinator_address) {
  return std::make_unique<CachedCoordinatorResolver>(coordinator_address);
}

}  // namespace network
}  // namespace gvdb
// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "core/status.h"
#include "internal.grpc.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <memory>
#include <string>
#include <vector>

namespace gvdb {
namespace storage { class SegmentManager; }
namespace cluster { class Coordinator; }
namespace network {

// Target for a distributed shard query
struct ShardTarget {
  uint32_t shard_id;
  uint32_t collection_id;
  std::string node_address;
};

// Information about a collection (used in ListCollections responses)
struct CollectionInfo {
  core::CollectionId collection_id;
  std::string collection_name;
  core::Dimension dimension;
  core::MetricType metric_type;
  core::IndexType index_type;
  uint64_t vector_count = 0;
};

// Interface for resolving collection metadata.
// Three implementations support the three VectorDBService operating modes:
//   - LocalCollectionResolver (single-node)
//   - CachedCoordinatorResolver (distributed data/query nodes)
//   - CoordinatorCollectionResolver (coordinator node)
class ICollectionResolver {
 public:
  virtual ~ICollectionResolver() = default;

  // Resolve a collection name to its ID
  virtual absl::StatusOr<core::CollectionId> GetCollectionId(
      const std::string& name) = 0;

  // Create a new collection, returning its ID
  virtual absl::StatusOr<core::CollectionId> CreateCollection(
      const std::string& name,
      core::Dimension dimension,
      core::MetricType metric_type,
      core::IndexType index_type,
      size_t num_shards = 1) = 0;

  // Drop a collection by name
  virtual absl::Status DropCollection(const std::string& name) = 0;

  // List all collections
  virtual std::vector<CollectionInfo> ListCollections() = 0;

  // Get the total number of collections (for metrics)
  virtual size_t CollectionCount() = 0;

  // Get the segment ID for a collection (needed by data operations)
  // Single-node: returns the allocated segment ID
  // Distributed: returns SegmentId(collection_id) (Phase 5 simplification)
  // Coordinator: returns error (data ops not supported)
  virtual absl::StatusOr<core::SegmentId> GetSegmentId(
      const std::string& name) = 0;

  // Whether this resolver supports data operations (Insert/Search/Get/Delete)
  virtual bool SupportsDataOps() const = 0;

  // Get all segment IDs for a collection (for multi-shard search)
  virtual absl::StatusOr<std::vector<core::SegmentId>> GetSegmentIds(
      const std::string& name) {
    auto seg = GetSegmentId(name);
    if (!seg.ok()) return seg.status();
    return std::vector<core::SegmentId>{*seg};
  }

  // Get number of shards for a collection
  virtual size_t GetNumShards(const std::string& name) { return 1; }

  // Get gRPC stub for segment replication (only CachedCoordinatorResolver has one)
  virtual proto::internal::InternalService::Stub* GetCoordinatorStub() { return nullptr; }

  // Get shard targets for distributed search (calls RouteQuery on coordinator)
  // Returns empty if not a distributed resolver
  virtual absl::StatusOr<std::vector<ShardTarget>> GetShardTargets(
      const std::string& collection_name) {
    return absl::UnimplementedError("Not a distributed resolver");
  }
};

// Factory functions for creating resolvers
std::unique_ptr<ICollectionResolver> MakeLocalResolver(
    std::shared_ptr<storage::SegmentManager> segment_manager);

std::unique_ptr<ICollectionResolver> MakeCoordinatorResolver(
    std::shared_ptr<cluster::Coordinator> coordinator);

std::unique_ptr<ICollectionResolver> MakeCachedCoordinatorResolver(
    const std::string& coordinator_address);

}  // namespace network
}  // namespace gvdb
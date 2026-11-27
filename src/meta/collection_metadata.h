// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The cluster's view of one collection: its schema, its shards and its size.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "core/types.h"

namespace gvdb::meta {

// Collection metadata that the coordinator stores and the data and query
// nodes cache.
struct CollectionMetadata {
  core::CollectionId collection_id;
  std::string collection_name;
  core::Dimension dimension;
  core::MetricType metric_type;
  core::IndexType index_type;

  // Sharding information
  std::vector<core::ShardId> shard_ids;
  size_t num_shards = 1;
  size_t replication_factor;

  // Stats
  uint64_t total_vectors;
  uint64_t total_size_bytes;
  core::Timestamp created_at;
  core::Timestamp updated_at;

  CollectionMetadata()
      : collection_id(core::kInvalidCollectionId),
        dimension(0),
        metric_type(core::MetricType::L2),
        index_type(core::IndexType::FLAT),
        replication_factor(1),
        total_vectors(0),
        total_size_bytes(0),
        created_at(0),
        updated_at(0) {}

  CollectionMetadata(core::CollectionId id, std::string name, core::Dimension dim,
                     core::MetricType metric, core::IndexType index)
      : collection_id(id),
        collection_name(std::move(name)),
        dimension(dim),
        metric_type(metric),
        index_type(index),
        replication_factor(1),
        total_vectors(0),
        total_size_bytes(0),
        created_at(0),
        updated_at(0) {}
};

}  // namespace gvdb::meta

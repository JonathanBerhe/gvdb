// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "core/metadata.h"
#include "core/types.h"
#include "service/vectordb/collection_resolver.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_store.h"

namespace gvdb::service {

// Finds the segments of a collection for the read and write handlers. On a
// distributed data or query node, a segment that is not on this node is
// pulled from the coordinator. The resolver must outlive the locator.
class SegmentLocator {
 public:
  SegmentLocator(std::shared_ptr<storage::ISegmentStore> segment_store,
                 ICollectionResolver& resolver);

  // The segment from the local store, or else pulled from the coordinator
  // when the resolver has a coordinator stub. Null when neither has it.
  storage::Segment* GetOrReplicate(core::SegmentId segment_id);

  // Every segment that may hold vectors of the collection: the shard-primary
  // segments of the resolver (pulled from the coordinator when not local),
  // then every rotated segment that the store tracks for the collection,
  // without duplicates and with the shard primaries first. NotFound when the
  // collection resolves to no segment.
  absl::StatusOr<std::vector<storage::Segment*>> CollectionSegments(
      const std::string& collection_name);

  // The metadata of `id` from the first segment in `segment_ids` that has
  // it, or nullopt when no segment has it.
  [[nodiscard]] std::optional<core::Metadata> FindMetadata(
      core::VectorId id, const std::vector<core::SegmentId>& segment_ids) const;

 private:
  std::shared_ptr<storage::ISegmentStore> segment_store_;
  ICollectionResolver& resolver_;
};

}  // namespace gvdb::service

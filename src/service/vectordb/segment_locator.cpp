// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/vectordb/segment_locator.h"

#include <grpcpp/grpcpp.h>

#include <unordered_set>
#include <utility>

#include "common/logger.h"
#include "internal.grpc.pb.h"

namespace gvdb::service {

SegmentLocator::SegmentLocator(std::shared_ptr<storage::ISegmentStore> segment_store,
                               ICollectionResolver& resolver)
    : segment_store_(std::move(segment_store)), resolver_(resolver) {}

storage::Segment* SegmentLocator::GetOrReplicate(core::SegmentId segment_id) {
  auto* segment = segment_store_->GetSegment(segment_id);
  if (segment) return segment;

  // Segment not found locally: try pulling from coordinator if available
  auto* stub = resolver_.GetCoordinatorStub();
  if (!stub) return nullptr;

  common::Logger::Instance().Info("Segment {} not found locally, pulling from coordinator",
                                  static_cast<uint64_t>(core::ToUInt32(segment_id)));

  grpc::ClientContext context;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(static_cast<uint64_t>(core::ToUInt32(segment_id)));
  proto::internal::GetSegmentResponse response;

  auto grpc_status = stub->GetSegment(&context, request, &response);
  if (!grpc_status.ok()) {
    common::Logger::Instance().Error("Failed to fetch segment {} from coordinator: {}",
                                     static_cast<uint64_t>(core::ToUInt32(segment_id)),
                                     grpc_status.error_message());
    return nullptr;
  }

  if (!response.has_segment_info() || response.segment_data().empty()) {
    common::Logger::Instance().Warn("Segment {} not found on coordinator",
                                    static_cast<uint64_t>(core::ToUInt32(segment_id)));
    return nullptr;
  }

  auto segment_result = storage::Segment::DeserializeFromBytes(response.segment_data());
  if (!segment_result.ok()) {
    common::Logger::Instance().Error("Failed to deserialize segment {}: {}",
                                     response.segment_info().segment_id(),
                                     segment_result.status().message());
    return nullptr;
  }

  auto add_status = segment_store_->AddReplicatedSegment(std::move(segment_result.value()));
  if (!add_status.ok()) {
    common::Logger::Instance().Error("Failed to add replicated segment {}: {}",
                                     response.segment_info().segment_id(), add_status.message());
    return nullptr;
  }

  common::Logger::Instance().Info("Replicated segment {} ({} vectors)",
                                  response.segment_info().segment_id(),
                                  response.segment_info().vector_count());

  return segment_store_->GetSegment(segment_id);
}

absl::StatusOr<std::vector<storage::Segment*>> SegmentLocator::CollectionSegments(
    const std::string& collection_name) {
  // The resolver knows the shard-primary segments but not the rotated ones
  // auto-seal created; those live in the store keyed by collection id. Start
  // from the primaries (so the replica-fallback pull still applies to them),
  // then add every segment the store tracks for the collection.
  auto segment_ids_result = resolver_.GetSegmentIds(collection_name);
  if (!segment_ids_result.ok()) {
    return segment_ids_result.status();
  }
  const auto& shard_segment_ids = *segment_ids_result;
  if (shard_segment_ids.empty()) {
    return absl::NotFoundError("Segment not found");
  }

  std::vector<storage::Segment*> segments;
  std::unordered_set<uint32_t> seen;
  bool have_collection = false;
  core::CollectionId collection_id{};

  for (const auto& seg_id : shard_segment_ids) {
    auto* segment = GetOrReplicate(seg_id);
    if (!segment) continue;
    if (!have_collection) {
      collection_id = segment->GetCollectionId();
      have_collection = true;
    }
    if (seen.insert(core::ToUInt32(seg_id)).second) {
      segments.push_back(segment);
    }
  }

  if (!have_collection) {
    return absl::NotFoundError("Segment not found");
  }

  for (const auto& seg_id : segment_store_->GetCollectionSegments(collection_id)) {
    if (!seen.insert(core::ToUInt32(seg_id)).second) continue;
    auto* segment = segment_store_->GetSegment(seg_id);
    if (segment) segments.push_back(segment);
  }

  return segments;
}

std::optional<core::Metadata> SegmentLocator::FindMetadata(
    core::VectorId id, const std::vector<core::SegmentId>& segment_ids) const {
  for (auto seg_id : segment_ids) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;
    auto meta = segment->GetMetadata(id);
    if (meta.ok()) return std::move(*meta);
  }
  return std::nullopt;
}

}  // namespace gvdb::service

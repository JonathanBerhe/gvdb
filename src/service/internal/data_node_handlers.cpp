// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// RPCs that a data node answers: segment replication and creation, shard
// queries, the two-phase primary swap, shard backup and restore, and the
// backup write fence.

#include "cluster/datanode/primary_term_tracker.h"
#include "cluster/datanode/shard_write_gate.h"
#include "common/logger.h"
#include "core/type_names.h"
#include "core/types.h"
#include "rpc/proto_conversions.h"
#include "service/internal/internal_service.h"
#include "storage/backup/backup_manager.h"
#include "storage/backup/restore_manager.h"

namespace gvdb::service {

// =============================================================================
// Segment Replication
// =============================================================================

grpc::Status InternalService::ReplicateSegment(
    grpc::ServerContext* /*context*/, const proto::internal::ReplicateSegmentRequest* request,
    proto::internal::ReplicateSegmentResponse* response) {
  try {
    const auto& segment_info = request->segment_info();
    uint64_t segment_id = segment_info.segment_id();
    uint32_t collection_id = segment_info.collection_id();
    uint32_t shard_id = segment_info.shard_id();
    const auto& segment_data = request->segment_data();

    common::Logger::Instance().Info(
        "ReplicateSegment: segment={}, collection={}, shard={}, data_size={}", segment_id,
        collection_id, shard_id, segment_data.size());

    // Validate segment info
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    if (segment_data.empty()) {
      response->set_success(false);
      response->set_message("Empty segment_data");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Empty segment_data");
    }

    // Deserialize segment from bytes
    auto segment_result = storage::Segment::DeserializeFromBytes(segment_data);
    if (!segment_result.ok()) {
      response->set_success(false);
      response->set_message(
          absl::StrFormat("Failed to deserialize segment: %s",
                          std::string(segment_result.status().message()).c_str()));
      common::Logger::Instance().Error("Deserialization failed: {}",
                                       segment_result.status().message());
      return grpc::Status::OK;
    }

    // Add to segment manager
    auto add_status = segment_store_->AddReplicatedSegment(std::move(segment_result.value()));
    if (!add_status.ok()) {
      response->set_success(false);
      response->set_message(
          absl::StrFormat("Failed to add segment: %s", std::string(add_status.message()).c_str()));
      common::Logger::Instance().Error("AddReplicatedSegment failed: {}", add_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu replicated successfully (%lu bytes)",
                                          segment_id, segment_data.size()));

    common::Logger::Instance().Info("ReplicateSegment completed: segment={}, {} bytes", segment_id,
                                    segment_data.size());
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("ReplicateSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::GetSegment(grpc::ServerContext* /*context*/,
                                         const proto::internal::GetSegmentRequest* request,
                                         proto::internal::GetSegmentResponse* response) {
  try {
    uint64_t segment_id = request->segment_id();

    common::Logger::Instance().Debug("GetSegment: segment={}", segment_id);

    // Validate segment_id
    if (segment_id == 0) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    // Try to get segment from SegmentManager
    auto seg_id = static_cast<core::SegmentId>(segment_id);
    auto segment = segment_store_->GetSegment(seg_id);
    if (!segment) {
      common::Logger::Instance().Debug("GetSegment: segment={} not found", segment_id);
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          absl::StrFormat("Segment %lu not found", segment_id));
    }

    // Fill response with segment info
    auto* segment_info = response->mutable_segment_info();
    segment_info->set_segment_id(static_cast<uint64_t>(segment->GetId()));
    segment_info->set_collection_id(core::ToUInt32(segment->GetCollectionId()));
    segment_info->set_shard_id(0);  // TODO: Track shard_id in segment metadata
    segment_info->set_vector_count(segment->GetVectorCount());

    // Serialize segment data
    auto serialize_result = segment->SerializeToBytes();
    if (!serialize_result.ok()) {
      common::Logger::Instance().Error("GetSegment: serialization failed: {}",
                                       serialize_result.status().message());
      return grpc::Status(
          grpc::StatusCode::INTERNAL,
          absl::StrFormat("Failed to serialize segment: %s",
                          std::string(serialize_result.status().message()).c_str()));
    }

    // Set segment data
    const auto& data = serialize_result.value();
    response->set_segment_data(data);

    common::Logger::Instance().Debug("GetSegment: found segment={} with {} vectors ({} bytes)",
                                     segment_id, segment->GetVectorCount(), data.size());
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("GetSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::ListSegments(grpc::ServerContext* /*context*/,
                                           const proto::internal::ListSegmentsRequest* request,
                                           proto::internal::ListSegmentsResponse* response) {
  try {
    uint32_t collection_id = request->collection_id();
    uint32_t shard_id = request->shard_id();

    common::Logger::Instance().Debug("ListSegments: collection={}, shard={}", collection_id,
                                     shard_id);

    // List segments, optionally filtered by collection_id
    std::vector<core::SegmentId> segment_ids;

    if (collection_id > 0) {
      segment_ids = segment_store_->GetCollectionSegments(core::MakeCollectionId(collection_id));
    } else {
      // Get all segments by iterating all collections
      // SegmentManager doesn't expose a GetAllSegments, so use GetSegment
      // to check known IDs. For now, return segments for all known collections.
      // This is a best-effort approach.
      segment_ids = segment_store_->GetCollectionSegments(
          core::MakeCollectionId(0));  // Will return empty if no collection 0
    }

    for (const auto& seg_id : segment_ids) {
      auto* segment = segment_store_->GetSegment(seg_id);
      if (!segment) continue;

      auto* info = response->add_segments();
      info->set_segment_id(static_cast<uint64_t>(core::ToUInt32(seg_id)));
      info->set_collection_id(collection_id);
      info->set_vector_count(segment->GetVectorCount());
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("ListSegments failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::DeleteSegment(grpc::ServerContext* /*context*/,
                                            const proto::internal::DeleteSegmentRequest* request,
                                            proto::internal::DeleteSegmentResponse* response) {
  try {
    uint64_t segment_id = request->segment_id();
    bool force = request->force();

    common::Logger::Instance().Info("DeleteSegment: segment={}, force={}", segment_id, force);

    // Validate segment_id
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    // Check if segment exists
    auto seg_id = static_cast<core::SegmentId>(segment_id);
    auto segment = segment_store_->GetSegment(seg_id);
    if (!segment) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Segment %lu not found", segment_id));
      return grpc::Status::OK;
    }

    // Check segment state before deletion
    auto state = segment->GetState();
    common::Logger::Instance().Info("DeleteSegment: segment={} state={}", segment_id,
                                    static_cast<int>(state));

    // Delete segment from memory and disk
    auto drop_status = segment_store_->DropSegment(seg_id, true /* delete_files */);
    if (!drop_status.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to delete segment %lu: %s", segment_id,
                                            std::string(drop_status.message()).c_str()));
      common::Logger::Instance().Error("DeleteSegment failed: {}", drop_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat(
        "Segment %lu deleted successfully (removed from memory and disk)", segment_id));
    common::Logger::Instance().Info("DeleteSegment: successfully deleted segment={}", segment_id);

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("DeleteSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::CreateSegment(grpc::ServerContext* /*context*/,
                                            const proto::internal::CreateSegmentRequest* request,
                                            proto::internal::CreateSegmentResponse* response) {
  try {
    uint64_t segment_id = request->segment_id();
    uint32_t collection_id = request->collection_id();
    uint32_t dimension = request->dimension();
    const std::string& metric_type_str = request->metric_type();
    const std::string& index_type_str = request->index_type();

    common::Logger::Instance().Info(
        "CreateSegment: segment={}, collection={}, dimension={}, metric={}, index={}", segment_id,
        collection_id, dimension, metric_type_str, index_type_str);

    // Validate inputs
    if (segment_id == 0) {
      response->set_success(false);
      response->set_message("Invalid segment_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid segment_id");
    }

    if (collection_id == 0) {
      response->set_success(false);
      response->set_message("Invalid collection_id");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid collection_id");
    }

    if (dimension == 0) {
      response->set_success(false);
      response->set_message("Invalid dimension");
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid dimension");
    }

    // Convert string types to enums
    auto metric_result = core::MetricTypeFromString(metric_type_str);
    if (!metric_result.ok()) {
      response->set_success(false);
      response->set_message(std::string(metric_result.status().message()));
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          std::string(metric_result.status().message()));
    }

    auto index_result = core::IndexTypeFromString(index_type_str);
    if (!index_result.ok()) {
      response->set_success(false);
      response->set_message(std::string(index_result.status().message()));
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          std::string(index_result.status().message()));
    }

    // Create segment
    auto seg_id = static_cast<core::SegmentId>(segment_id);
    core::CollectionId coll_id = core::MakeCollectionId(collection_id);
    auto dim = static_cast<core::Dimension>(dimension);

    auto create_status =
        segment_store_->CreateSegmentWithId(seg_id, coll_id, dim, *metric_result, *index_result);

    if (!create_status.ok()) {
      response->set_success(false);
      response->set_message(absl::StrFormat("Failed to create segment: %s",
                                            std::string(create_status.message()).c_str()));
      common::Logger::Instance().Error("CreateSegment failed: {}", create_status.message());
      return grpc::Status::OK;
    }

    response->set_success(true);
    response->set_message(absl::StrFormat("Segment %lu created successfully", segment_id));

    common::Logger::Instance().Info("CreateSegment: successfully created segment={}", segment_id);
    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("CreateSegment failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

grpc::Status InternalService::ExecuteShardQuery(
    grpc::ServerContext* /*context*/, const proto::internal::ExecuteShardQueryRequest* request,
    proto::internal::ExecuteShardQueryResponse* response) {
  try {
    uint32_t collection_id = request->collection_id();
    uint32_t top_k = request->top_k();

    common::Logger::Instance().Debug("ExecuteShardQuery: collection={}, top_k={}", collection_id,
                                     top_k);

    uint32_t shard_id = request->shard_id();
    core::SegmentId segment_id =
        core::ShardSegmentId(core::MakeCollectionId(collection_id), shard_id);
    auto* segment = segment_store_->GetSegment(segment_id);
    if (!segment) {
      return grpc::Status(grpc::StatusCode::NOT_FOUND,
                          absl::StrCat("Segment not found for collection ", collection_id));
    }

    // Convert query vector from proto
    if (request->query_vector().empty()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Query vector is empty");
    }

    std::vector<float> query_data(request->query_vector().begin(), request->query_vector().end());
    core::Vector query(query_data);

    // Validate dimension
    if (query.dimension() != segment->GetDimension()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          absl::StrCat("Query dimension mismatch: expected ",
                                       segment->GetDimension(), ", got ", query.dimension()));
    }

    // Execute search
    absl::StatusOr<core::SearchResult> search_result;

    if (!request->filter().empty()) {
      search_result = segment->SearchWithFilter(query, top_k, request->filter());
    } else {
      search_result = segment->Search(query, top_k);
    }

    if (!search_result.ok()) {
      return grpc::Status(grpc::StatusCode::INTERNAL,
                          std::string(search_result.status().message()));
    }

    // Convert results to proto
    for (const auto& entry : search_result->entries) {
      auto* result = response->add_results();
      result->set_id(core::ToUInt64(entry.id));
      result->set_distance(entry.distance);

      if (request->return_metadata()) {
        auto meta_result = segment->GetMetadata(entry.id);
        if (meta_result.ok()) {
          rpc::ToProto(*meta_result, result->mutable_metadata());
        }
      }
    }

    return grpc::Status::OK;

  } catch (const std::exception& e) {
    common::Logger::Instance().Error("ExecuteShardQuery failed: {}", e.what());
    return grpc::Status(grpc::StatusCode::INTERNAL, e.what());
  }
}

// =============================================================================
// Two-Phase Primary Swap
// =============================================================================
//
// Both handlers are idempotent: a coordinator that retries after a transient
// failure must not corrupt the local PrimaryTermTracker state.
//
//   PausePrimary: stop accepting writes for `shard_id`. Records
//     last_known_term = new_term - 1 so a subsequent stale-routing write
//     tagged with `term ≤ last_known_term` is rejected with the right
//     error. Replay (already-not-primary) returns OK.
//
//   PreparePromote: claim primary status at `new_term`. Strictly
//     monotonic: re-promote at an equal or older term returns
//     success=false so a buggy double-promote surfaces loudly. Replay
//     at the same term is OK because RecordPrimary is idempotent on
//     (shard, term) match.
//
// Both calls are no-ops on a non-data-node host (tracker is null);
// they return FAILED_PRECONDITION rather than crashing so a misrouted
// RPC is surfaced to the caller.

grpc::Status InternalService::PausePrimary(grpc::ServerContext* /*context*/,
                                           const proto::internal::PausePrimaryRequest* request,
                                           proto::internal::PausePrimaryResponse* response) {
  if (!primary_term_tracker_) {
    response->set_success(false);
    response->set_message("PausePrimary received on a non-data-node host");
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "primary_term_tracker not wired on this service");
  }

  const uint32_t shard_id = request->shard_id();
  const uint64_t new_term = request->new_term();
  const uint32_t new_primary = request->new_primary_node_id();

  if (new_term == 0) {
    response->set_success(false);
    response->set_message("new_term must be > 0");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "PausePrimary: new_term must be > 0");
  }

  // last_known_term is the term we're being demoted FROM, i.e. the
  // cluster's current term before the swap commits. By contract that's
  // exactly new_term - 1.
  const uint64_t last_known_term = new_term - 1;
  const bool ok = primary_term_tracker_->RecordNotPrimary(shard_id, last_known_term);

  common::Logger::Instance().Info(
      "PausePrimary: shard={} stepping_down_from_term={} new_primary_node={} record_ok={}",
      shard_id, last_known_term, new_primary, ok);

  // RecordNotPrimary returns false only on term regression: a coordinator
  // bug or a stale retry from a swap that already advanced beyond this
  // term. Either way the local view is already correct (or fresher), so
  // surface the regression as success=true with a diagnostic message
  // rather than failing the swap. Idempotency depends on this.
  response->set_success(true);
  if (!ok) {
    response->set_message(
        "term regression detected; local view already at or beyond new_term, idempotent ok");
  } else {
    response->set_message("paused");
  }
  return grpc::Status::OK;
}

grpc::Status InternalService::PreparePromote(grpc::ServerContext* /*context*/,
                                             const proto::internal::PreparePromoteRequest* request,
                                             proto::internal::PreparePromoteResponse* response) {
  if (!primary_term_tracker_) {
    response->set_success(false);
    response->set_message("PreparePromote received on a non-data-node host");
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "primary_term_tracker not wired on this service");
  }

  const uint32_t shard_id = request->shard_id();
  const uint64_t new_term = request->new_term();

  if (new_term == 0) {
    response->set_success(false);
    response->set_message("new_term must be > 0");
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "PreparePromote: new_term must be > 0");
  }

  const auto snap_before = primary_term_tracker_->Get(shard_id);
  const bool ok = primary_term_tracker_->RecordPrimary(shard_id, new_term);

  if (!ok) {
    // Strictly-greater term regression. This is a real coordinator bug:
    // surface it as a hard failure so the orchestrator's reconcile loop
    // logs it and gives up rather than silently overwriting state.
    // Idempotent replay at the SAME term goes through RecordPrimary's
    // ok-on-match path (returns true), so we won't reach this branch
    // for a benign retry.
    response->set_success(false);
    response->set_message(
        "term regression: refusing to promote at term <= currently-recorded term");
    common::Logger::Instance().Warn(
        "PreparePromote rejected: shard={} requested_term={} known_term={} (regression)", shard_id,
        new_term, snap_before.term);
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "PreparePromote: term must be strictly greater than known term");
  }

  common::Logger::Instance().Info(
      "PreparePromote: shard={} promoted_to_term={} (was_primary={}, prev_term={})", shard_id,
      new_term, snap_before.is_primary, snap_before.term);

  response->set_success(true);
  response->set_message("promoted");
  return grpc::Status::OK;
}

// =============================================================================
// Backup / Restore Orchestration (Coordinator → Data Node)
// =============================================================================

grpc::Status InternalService::BackupShard(grpc::ServerContext* /*context*/,
                                          const proto::internal::BackupShardRequest* request,
                                          proto::internal::BackupShardResponse* response) {
  if (!backup_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "Backup is not configured on this server");
  }
  if (!segment_store_) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "BackupShard requires a segment store");
  }

  auto target = rpc::FromProto(request->target());
  if (!target.ok()) {
    return rpc::ToGrpcStatus(target.status());
  }

  auto result = backup_manager_->RunShardBackup(
      request->backup_id(), core::MakeCollectionId(request->collection_id()),
      core::MakeShardId(static_cast<uint16_t>(request->shard_id())),
      /*primary_term=*/0, *target, segment_store_);
  if (!result.ok()) {
    response->set_success(false);
    response->set_message(std::string(result.status().message()));
    return rpc::ToGrpcStatus(result.status());
  }

  response->set_success(true);
  response->set_message("Shard backup complete");
  response->set_shard_manifest_key(result->shard_manifest_key);
  response->set_bytes_uploaded(result->bytes_uploaded);
  for (const auto& seg : result->segments) {
    auto* entry = response->add_segments();
    entry->set_segment_id(seg.segment_id);
    entry->set_is_growing(seg.is_growing);
    entry->set_vector_count(seg.vector_count);
    entry->set_size_bytes(seg.size_bytes);
    for (const auto& k : seg.object_keys) entry->add_object_keys(k);
  }
  return grpc::Status::OK;
}

grpc::Status InternalService::RestoreShard(grpc::ServerContext* /*context*/,
                                           const proto::internal::RestoreShardRequest* request,
                                           proto::internal::RestoreShardResponse* response) {
  if (!restore_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Restore is not configured on this server");
  }
  if (!segment_store_) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "RestoreShard requires a segment store");
  }

  auto source = rpc::FromProto(request->source());
  if (!source.ok()) {
    return rpc::ToGrpcStatus(source.status());
  }

  auto status = restore_manager_->RunShardRestore(
      *source, request->backup_id(), core::MakeCollectionId(request->target_collection_id()),
      core::MakeShardId(static_cast<uint16_t>(request->shard_id())), segment_store_);
  if (!status.ok()) {
    response->set_success(false);
    response->set_message(std::string(status.message()));
    return rpc::ToGrpcStatus(status);
  }
  response->set_success(true);
  response->set_message("Shard restore complete");
  // Segment count is captured by the per-shard manifest the caller already
  // wrote at backup time; we surface zero here and rely on GetRestoreStatus
  // for progress reporting in a multi-shard restore.
  return grpc::Status::OK;
}

grpc::Status InternalService::FreezeWrites(grpc::ServerContext* /*context*/,
                                           const proto::internal::FreezeWritesRequest* request,
                                           proto::internal::FreezeWritesResponse* response) {
  if (!shard_write_gate_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Write fence is not configured on this server");
  }
  if (request->fence_token().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "fence_token is required");
  }
  if (request->lease_ms() <= 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "lease_ms must be positive");
  }
  const auto shard_id = core::MakeShardId(static_cast<uint16_t>(request->shard_id()));
  const bool ok = shard_write_gate_->Freeze(shard_id, request->fence_token(), request->lease_ms());
  response->set_success(ok);
  if (!ok) {
    response->set_message("Shard already frozen by another backup token");
    return grpc::Status::OK;
  }
  response->set_message("Writes frozen");
  return grpc::Status::OK;
}

grpc::Status InternalService::UnfreezeWrites(grpc::ServerContext* /*context*/,
                                             const proto::internal::UnfreezeWritesRequest* request,
                                             proto::internal::UnfreezeWritesResponse* response) {
  if (!shard_write_gate_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                        "Write fence is not configured on this server");
  }
  if (request->fence_token().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "fence_token is required");
  }
  const auto shard_id = core::MakeShardId(static_cast<uint16_t>(request->shard_id()));
  const bool ok = shard_write_gate_->Unfreeze(shard_id, request->fence_token());
  response->set_success(ok);
  response->set_message(ok ? "Writes unfrozen" : "No matching freeze for token");
  return grpc::Status::OK;
}

}  // namespace gvdb::service

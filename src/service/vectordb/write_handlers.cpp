// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Insert, stream insert, upsert, delete and metadata update.

#include "service/vectordb/vectordb_service.h"
#include "rpc/proto_conversions.h"
#include "service/vectordb/collection_resolver.h"
#include "auth/audit_context.h"
#include "storage/segment/batch_splitter.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace service {

// ============================================================================
// Vector Operations
// ============================================================================

grpc::Status VectorDBService::Insert(
    grpc::ServerContext* context,
    const proto::InsertRequest* request,
    proto::InsertResponse* response) {
  auto perm = CheckPermission(auth::Permission::INSERT, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());
  auth::AuditContext::SetItemCount(request->vectors().size());

  common::Logger::Instance().Info("Insert: {} vectors into {}",
                                 request->vectors().size(),
                                 request->collection_name());

  common::MetricsRegistry::Instance().RecordBatchSize(request->vectors().size());

  common::MetricsTimer timer(
      common::MetricsRegistry::Instance(),
      common::MetricsTimer::OperationType::INSERT,
      request->collection_name());

  constexpr size_t MAX_BATCH_SIZE = 50000;

  if (static_cast<size_t>(request->vectors().size()) > MAX_BATCH_SIZE) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Batch size exceeds maximum (", MAX_BATCH_SIZE,
                     " vectors). Current batch: ", request->vectors().size()));
  }

  if (request->vectors().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Cannot insert empty vector batch");
  }

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Insert operations not supported on coordinator nodes. "
        "Send insert requests to data nodes instead.");
  }

  // Primary-term gate. Use the first vector's id as the shard sample:
  // the proxy splits per-shard before sending so all vectors in an
  // RPC share a shard, but even if a malformed client sends mixed
  // shards the gate at least catches the dominant case.
  if (auto gate = EvaluateWriteGate(context, request->collection_name(),
                                     request->vectors(0).id());
      !gate.ok()) {
    return gate;
  }

  // Get all segment IDs for this collection
  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::toGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;
  size_t num_shards = segment_ids.size();

  // Get dimension from first segment
  auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
  if (!first_segment) {
    return rpc::toGrpcStatus(absl::NotFoundError("Segment not found"));
  }
  uint32_t dimension = first_segment->GetDimension();

  // Convert proto vectors and group by shard
  struct ShardBatch {
    std::vector<core::Vector> vectors;
    std::vector<core::VectorId> ids;
    std::vector<core::Metadata> metadata;
    std::unordered_map<uint64_t, core::SparseVector> sparse_map;
    std::unordered_map<uint64_t, int64_t> expiry_entries;
    bool has_metadata = false;
    bool has_sparse = false;
  };
  std::vector<ShardBatch> shard_batches(num_shards);

  for (const auto& proto_vec : request->vectors()) {
    auto vec_result = rpc::fromProto(proto_vec);
    if (!vec_result.ok()) return rpc::toGrpcStatus(vec_result.status());

    core::VectorId vid = vec_result->first;
    uint32_t shard_idx = static_cast<uint32_t>(
        core::ToUInt64(vid) % num_shards);

    auto& batch = shard_batches[shard_idx];
    batch.ids.push_back(vid);
    batch.vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      batch.has_metadata = true;
      auto meta_result = rpc::fromProto(proto_vec.metadata());
      if (!meta_result.ok()) return rpc::toGrpcStatus(meta_result.status());
      batch.metadata.push_back(std::move(*meta_result));
    } else if (batch.has_metadata) {
      batch.metadata.push_back(core::Metadata{});
    }

    if (proto_vec.has_sparse_vector() && proto_vec.sparse_vector().indices_size() > 0) {
      auto sparse_result = rpc::fromProto(proto_vec.sparse_vector());
      if (!sparse_result.ok()) return rpc::toGrpcStatus(sparse_result.status());
      batch.has_sparse = true;
      batch.sparse_map[core::ToUInt64(vid)] = std::move(*sparse_result);
    }

    if (proto_vec.ttl_seconds() > 0) {
      int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      batch.expiry_entries[core::ToUInt64(vid)] = now + static_cast<int64_t>(proto_vec.ttl_seconds());
    }
  }

  // Validate dimension
  for (const auto& batch : shard_batches) {
    for (const auto& vec : batch.vectors) {
      if (static_cast<uint32_t>(vec.dimension()) != dimension) {
        return rpc::toGrpcStatus(absl::InvalidArgumentError(
            absl::StrFormat("Vector dimension mismatch: expected %d, got %d",
                            dimension, vec.dimension())));
      }
    }
  }

  // Write each shard batch to its segment (with auto-rotation on full).
  // Batches larger than the segment size cap are split into ranges that each
  // fit an empty segment; without the split, rotation can never produce a
  // segment big enough and the write would fail unconditionally.
  core::CollectionId collection_id = first_segment->GetCollectionId();
  const size_t max_segment_bytes = segment_store_->GetMaxSegmentSize();
  size_t total_inserted = 0;
  for (uint32_t i = 0; i < num_shards; ++i) {
    auto& batch = shard_batches[i];
    if (batch.ids.empty()) continue;

    // Per-item cost must mirror the segment's own capacity check: the
    // metadata/sparse paths charge a flat per-item metadata estimate on top
    // of the vector bytes.
    const bool charges_metadata = batch.has_sparse || batch.has_metadata;
    std::vector<size_t> item_costs;
    item_costs.reserve(batch.vectors.size());
    for (const auto& v : batch.vectors) {
      item_costs.push_back(
          v.byte_size() +
          (charges_metadata ? storage::Segment::kMetadataBytesEstimate : 0));
    }
    auto ranges_or = storage::SplitBatchBySize(item_costs, max_segment_bytes);
    if (!ranges_or.ok()) {
      common::MetricsRegistry::Instance().RecordInsert(
          request->collection_name(), false, 0);
      return rpc::toGrpcStatus(ranges_or.status());
    }

    const bool single_range = ranges_or->size() == 1;
    for (const auto& [begin, end] : *ranges_or) {
      size_t range_bytes = 0;
      for (size_t j = begin; j < end; ++j) range_bytes += item_costs[j];

      // The common case is one range covering the whole batch; write it
      // without copying. Slices are only materialized when a split happened.
      std::vector<core::Vector> sliced_vectors;
      std::vector<core::VectorId> sliced_ids;
      std::vector<core::Metadata> sliced_metadata;
      std::unordered_map<uint64_t, core::SparseVector> sliced_sparse;
      std::unordered_map<uint64_t, int64_t> sliced_expiry;
      if (!single_range) {
        sliced_vectors.assign(batch.vectors.begin() + begin,
                              batch.vectors.begin() + end);
        sliced_ids.assign(batch.ids.begin() + begin, batch.ids.begin() + end);
        if (batch.metadata.size() == batch.ids.size()) {
          sliced_metadata.assign(batch.metadata.begin() + begin,
                                 batch.metadata.begin() + end);
        } else {
          // Malformed parallel arrays (metadata missing on leading vectors).
          // Pass as-is so the segment's size-mismatch validation rejects it,
          // matching the unsplit path.
          sliced_metadata = batch.metadata;
        }
        for (const auto& vid : sliced_ids) {
          uint64_t key = core::ToUInt64(vid);
          if (auto it = batch.sparse_map.find(key);
              it != batch.sparse_map.end()) {
            sliced_sparse.emplace(key, it->second);
          }
          if (auto it = batch.expiry_entries.find(key);
              it != batch.expiry_entries.end()) {
            sliced_expiry.emplace(key, it->second);
          }
        }
      }
      const auto& range_vectors = single_range ? batch.vectors : sliced_vectors;
      const auto& range_ids = single_range ? batch.ids : sliced_ids;
      const auto& range_metadata =
          single_range ? batch.metadata : sliced_metadata;
      const auto& range_sparse = single_range ? batch.sparse_map : sliced_sparse;
      const auto& range_expiry =
          single_range ? batch.expiry_entries : sliced_expiry;

      // Get a writable segment that can fit this range
      auto* segment =
          segment_store_->GetWritableSegment(collection_id, range_bytes);
      if (!segment) {
        segment = segment_store_->GetSegment(segment_ids[i]);
      }
      if (!segment) break;

      auto write_range = [&](storage::Segment* seg) {
        if (batch.has_sparse) {
          return seg->AddVectorsWithSparse(range_vectors, range_ids,
                                           range_metadata, range_sparse,
                                           range_expiry);
        }
        if (batch.has_metadata) {
          return seg->AddVectorsWithMetadata(range_vectors, range_ids,
                                             range_metadata, range_expiry);
        }
        return seg->AddVectors(range_vectors, range_ids, range_expiry);
      };

      absl::Status status = write_range(segment);

      // Safety net: if segment is full despite hint, rotate and retry once
      if (absl::IsResourceExhausted(status)) {
        segment = segment_store_->GetWritableSegment(collection_id, range_bytes);
        if (segment) {
          status = write_range(segment);
        }
      }

      if (!status.ok()) {
        common::MetricsRegistry::Instance().RecordInsert(
            request->collection_name(), false, 0);
        return rpc::toGrpcStatus(status);
      }

      total_inserted += range_ids.size();
    }
  }

  common::MetricsRegistry::Instance().RecordInsert(
      request->collection_name(), true, total_inserted);

  response->set_inserted_count(total_inserted);
  response->set_message("Vectors inserted successfully");

  // Invalidate query cache for this collection
  if (auto* cache = query_executor_->GetCache()) {
    auto cid = resolver_->GetCollectionId(request->collection_name());
    if (cid.ok()) cache->InvalidateCollection(core::ToUInt32(*cid));
  }

  return grpc::Status::OK;
}

grpc::Status VectorDBService::StreamInsert(
    grpc::ServerContext* /*context*/,
    grpc::ServerReader<proto::InsertRequest>* reader,
    proto::InsertResponse* response) {

  common::MetricsTimer timer(
      common::MetricsRegistry::Instance(),
      common::MetricsTimer::OperationType::INSERT,
      "stream");

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Insert operations not supported on coordinator nodes.");
  }

  uint64_t total_inserted = 0;
  proto::InsertRequest chunk;
  std::string collection_name;
  bool perm_checked = false;

  while (reader->Read(&chunk)) {
    if (chunk.vectors().empty()) continue;
    if (collection_name.empty()) collection_name = chunk.collection_name();
    if (!perm_checked) {
      auto perm = CheckPermission(auth::Permission::STREAM_INSERT, collection_name);
      if (!perm.ok()) return perm;
      auth::AuditContext::SetCollection(collection_name);
      perm_checked = true;
    }

    auto segment_ids_result = resolver_->GetSegmentIds(chunk.collection_name());
    if (!segment_ids_result.ok()) {
      return rpc::toGrpcStatus(segment_ids_result.status());
    }
    const auto& segment_ids = *segment_ids_result;
    size_t num_shards = segment_ids.size();

    auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
    if (!first_segment) {
      return rpc::toGrpcStatus(absl::NotFoundError("Segment not found"));
    }
    uint32_t dimension = first_segment->GetDimension();

    struct ShardBatch {
      std::vector<core::Vector> vectors;
      std::vector<core::VectorId> ids;
      std::vector<core::Metadata> metadata;
      bool has_metadata = false;
    };
    std::vector<ShardBatch> shard_batches(num_shards);

    for (const auto& proto_vec : chunk.vectors()) {
      auto vec_result = rpc::fromProto(proto_vec);
      if (!vec_result.ok()) return rpc::toGrpcStatus(vec_result.status());

      core::VectorId vid = vec_result->first;
      uint32_t shard_idx = static_cast<uint32_t>(
          core::ToUInt64(vid) % num_shards);

      auto& batch = shard_batches[shard_idx];
      batch.ids.push_back(vid);
      batch.vectors.push_back(std::move(vec_result->second));

      if (proto_vec.has_metadata()) {
        batch.has_metadata = true;
        auto meta_result = rpc::fromProto(proto_vec.metadata());
        if (!meta_result.ok()) return rpc::toGrpcStatus(meta_result.status());
        batch.metadata.push_back(std::move(*meta_result));
      } else if (batch.has_metadata) {
        batch.metadata.push_back(core::Metadata{});
      }
    }

    for (const auto& batch : shard_batches) {
      for (const auto& vec : batch.vectors) {
        if (static_cast<uint32_t>(vec.dimension()) != dimension) {
          return rpc::toGrpcStatus(absl::InvalidArgumentError(
              absl::StrFormat("Vector dimension mismatch: expected %d, got %d",
                              dimension, vec.dimension())));
        }
      }
    }

    for (uint32_t i = 0; i < num_shards; ++i) {
      auto& batch = shard_batches[i];
      if (batch.ids.empty()) continue;

      auto* segment = segment_store_->GetSegment(segment_ids[i]);
      if (!segment) continue;

      absl::Status status;
      if (batch.has_metadata) {
        status = segment->AddVectorsWithMetadata(batch.vectors, batch.ids, batch.metadata);
      } else {
        status = segment->AddVectors(batch.vectors, batch.ids);
      }

      if (!status.ok()) {
        common::MetricsRegistry::Instance().RecordInsert(collection_name, false, 0);
        return rpc::toGrpcStatus(status);
      }
      total_inserted += batch.ids.size();
    }
  }

  common::MetricsRegistry::Instance().RecordInsert(
      collection_name, true, total_inserted);
  common::MetricsRegistry::Instance().RecordBatchSize(total_inserted);

  response->set_inserted_count(total_inserted);
  response->set_message("Vectors inserted successfully");

  // Invalidate query cache
  if (auto* cache = query_executor_->GetCache()) {
    if (!collection_name.empty()) {
      auto cid = resolver_->GetCollectionId(collection_name);
      if (cid.ok()) cache->InvalidateCollection(core::ToUInt32(*cid));
    }
  }

  return grpc::Status::OK;
}

// ============================================================================
// Upsert
// ============================================================================

grpc::Status VectorDBService::Upsert(
    grpc::ServerContext* context,
    const proto::UpsertRequest* request,
    proto::UpsertResponse* response) {
  auto perm = CheckPermission(auth::Permission::UPSERT, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());
  auth::AuditContext::SetItemCount(request->vectors().size());
  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Collection name is required");
  }
  if (request->vectors().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "No vectors provided");
  }

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Upsert operations not supported on coordinator nodes");
  }

  // Primary-term gate. See ProxyService::Insert for the full
  // rationale; this matches the contract on every write RPC.
  if (auto gate = EvaluateWriteGate(context, request->collection_name(),
                                     request->vectors(0).id());
      !gate.ok()) {
    return gate;
  }

  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::toGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;

  // Find a GROWING segment
  storage::Segment* growing_segment = nullptr;
  for (auto seg_id : segment_ids) {
    auto* seg = segment_store_->GetSegment(seg_id);
    if (seg && seg->CanAcceptWrites()) {
      growing_segment = seg;
      break;
    }
  }
  if (!growing_segment) {
    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "No writable segment available");
  }

  // Convert proto vectors
  std::vector<core::Vector> vectors;
  std::vector<core::VectorId> ids;
  std::vector<core::Metadata> metadata;
  std::unordered_map<uint64_t, int64_t> expiry_entries;
  vectors.reserve(request->vectors_size());
  ids.reserve(request->vectors_size());
  metadata.reserve(request->vectors_size());

  for (const auto& proto_vec : request->vectors()) {
    auto vec_result = rpc::fromProto(proto_vec);
    if (!vec_result.ok()) {
      return rpc::toGrpcStatus(vec_result.status());
    }
    ids.push_back(vec_result->first);
    vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      auto meta_result = rpc::fromProto(proto_vec.metadata());
      if (!meta_result.ok()) {
        return rpc::toGrpcStatus(meta_result.status());
      }
      metadata.push_back(std::move(*meta_result));
    } else {
      metadata.push_back(core::Metadata{});
    }

    if (proto_vec.ttl_seconds() > 0) {
      int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();
      expiry_entries[core::ToUInt64(vec_result->first)] = now + static_cast<int64_t>(proto_vec.ttl_seconds());
    }
  }

  auto upsert_result = growing_segment->UpsertVectors(vectors, ids, metadata, expiry_entries);
  if (!upsert_result.ok()) {
    return rpc::toGrpcStatus(upsert_result.status());
  }

  response->set_upserted_count(upsert_result->inserted_count + upsert_result->updated_count);
  response->set_inserted_count(upsert_result->inserted_count);
  response->set_updated_count(upsert_result->updated_count);
  response->set_message("Upsert completed");

  common::Logger::Instance().Debug("Upsert: {} inserted, {} updated in collection '{}'",
      upsert_result->inserted_count, upsert_result->updated_count,
      request->collection_name());

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Delete(
    grpc::ServerContext* context,
    const proto::DeleteRequest* request,
    proto::DeleteResponse* response) {
  auto perm = CheckPermission(auth::Permission::DELETE, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());
  auth::AuditContext::SetItemCount(request->ids().size());

  common::Logger::Instance().Info("Delete: {} IDs from {}",
                                 request->ids().size(),
                                 request->collection_name());

  common::MetricsTimer timer(
      common::MetricsRegistry::Instance(),
      common::MetricsTimer::OperationType::DELETE,
      request->collection_name());

  if (request->ids().empty()) {
    common::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "IDs list cannot be empty");
  }

  constexpr size_t MAX_DELETE_BATCH_SIZE = 10000;
  if (static_cast<size_t>(request->ids().size()) > MAX_DELETE_BATCH_SIZE) {
    common::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Cannot delete more than ", MAX_DELETE_BATCH_SIZE,
                     " vectors in one request. Requested: ", request->ids().size()));
  }

  if (!resolver_->SupportsDataOps()) {
    common::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Delete operations not supported on coordinator nodes. "
        "Send delete requests to data nodes instead.");
  }

  // Primary-term gate. See ProxyService::Insert for the full
  // rationale; same contract on every write RPC.
  if (auto gate = EvaluateWriteGate(context, request->collection_name(),
                                     request->ids(0));
      !gate.ok()) {
    common::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return gate;
  }

  // A vector may live in any segment of the collection, so route each id to
  // the segment that actually holds it rather than the resolver's active one.
  auto segments_result = CollectCollectionSegments(request->collection_name());
  if (!segments_result.ok()) {
    common::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return rpc::toGrpcStatus(segments_result.status());
  }
  const auto& segments = *segments_result;

  std::vector<core::VectorId> remaining;
  remaining.reserve(request->ids().size());
  for (uint64_t id : request->ids()) {
    remaining.push_back(core::MakeVectorId(id));
  }

  uint64_t deleted_count = 0;
  for (auto* segment : segments) {
    if (remaining.empty()) break;

    // Only touch this segment for the ids it holds. Locating first keeps the
    // segment's own state rule intact: an id that lives in a sealed segment
    // routes here and DeleteVectors returns its FailedPrecondition error
    // rather than being silently reported as not found.
    auto present = segment->GetVectors(remaining, /*include_metadata=*/false);
    if (present.found_ids.empty()) continue;

    auto result = segment->DeleteVectors(present.found_ids);
    if (!result.ok()) {
      common::MetricsRegistry::Instance().RecordDelete(
          request->collection_name(), false);
      return rpc::toGrpcStatus(result.status());
    }
    deleted_count += result->deleted_count;

    std::unordered_set<uint64_t> just_deleted;
    just_deleted.reserve(present.found_ids.size());
    for (const auto& id : present.found_ids) {
      just_deleted.insert(core::ToUInt64(id));
    }
    std::vector<core::VectorId> next;
    next.reserve(remaining.size());
    for (const auto& id : remaining) {
      if (just_deleted.find(core::ToUInt64(id)) == just_deleted.end()) {
        next.push_back(id);
      }
    }
    remaining = std::move(next);
  }

  response->set_deleted_count(deleted_count);
  for (const auto& id : remaining) {
    response->add_not_found_ids(core::ToUInt64(id));
  }
  response->set_message(absl::StrCat(
      "Deleted ", deleted_count, " vector(s) from collection '",
      request->collection_name(), "'"));

  common::MetricsRegistry::Instance().RecordDelete(
      request->collection_name(), true);

  // Invalidate query cache
  if (auto* cache = query_executor_->GetCache()) {
    auto cid = resolver_->GetCollectionId(request->collection_name());
    if (cid.ok()) cache->InvalidateCollection(core::ToUInt32(*cid));
  }

  return grpc::Status::OK;
}

grpc::Status VectorDBService::UpdateMetadata(
    grpc::ServerContext* context,
    const proto::UpdateMetadataRequest* request,
    proto::UpdateMetadataResponse* response) {
  auto perm = CheckPermission(auth::Permission::UPDATE_METADATA, request->collection_name());
  if (!perm.ok()) return perm;
  auth::AuditContext::SetCollection(request->collection_name());

  common::Logger::Instance().Info("UpdateMetadata: ID {} in {}",
                                 request->id(), request->collection_name());

  common::MetricsTimer timer(
      common::MetricsRegistry::Instance(),
      common::MetricsTimer::OperationType::UPDATE_METADATA,
      request->collection_name());

  if (request->id() == 0) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Vector ID cannot be 0");
  }

  if (request->metadata().fields().empty()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Metadata cannot be empty");
  }

  if (!resolver_->SupportsDataOps()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "UpdateMetadata operations not supported on coordinator nodes. "
        "Send metadata update requests to data nodes instead.");
  }

  // Primary-term gate. See ProxyService::Insert for the full
  // rationale; same contract on every write RPC.
  if (auto gate = EvaluateWriteGate(context, request->collection_name(),
                                     request->id());
      !gate.ok()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return gate;
  }

  auto segments_result = CollectCollectionSegments(request->collection_name());
  if (!segments_result.ok()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return rpc::toGrpcStatus(segments_result.status());
  }
  const auto& segments = *segments_result;

  auto metadata_result = rpc::fromProto(request->metadata());
  if (!metadata_result.ok()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return rpc::toGrpcStatus(metadata_result.status());
  }

  auto vector_id = core::MakeVectorId(request->id());

  // Find the segment that actually holds the id, then apply the update there.
  // Locating first (state-independent) rather than trying each segment blindly
  // keeps a sealed segment's own FailedPrecondition error attached to the
  // segment that owns the id, and avoids a sealed segment that lacks the id
  // masking a later growing segment that has it.
  storage::Segment* owner = nullptr;
  for (auto* segment : segments) {
    auto present = segment->GetVectors({vector_id}, /*include_metadata=*/false);
    if (!present.found_ids.empty()) {
      owner = segment;
      break;
    }
  }

  if (!owner) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    auto not_found = absl::NotFoundError(absl::StrCat(
        "Vector ID ", request->id(), " not found in collection '",
        request->collection_name(), "'"));
    response->set_updated(false);
    response->set_message(std::string(not_found.message()));
    return rpc::toGrpcStatus(not_found);
  }

  auto status = owner->UpdateMetadata(vector_id, *metadata_result, request->merge());

  if (!status.ok()) {
    common::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    response->set_updated(false);
    response->set_message(std::string(status.message()));
    return rpc::toGrpcStatus(status);
  }

  common::MetricsRegistry::Instance().RecordUpdateMetadata(
      request->collection_name(), true);

  response->set_updated(true);
  response->set_message(absl::StrCat(
      "Updated metadata for vector ID ", request->id(),
      " in collection '", request->collection_name(), "'"));

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gvdb

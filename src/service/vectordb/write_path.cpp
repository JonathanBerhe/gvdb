// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "service/vectordb/write_path.h"

#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "cluster/datanode/primary_term_tracker.h"
#include "cluster/datanode/shard_write_gate.h"
#include "common/logger.h"
#include "common/metrics.h"
#include "core/vector.h"
#include "rpc/proto_conversions.h"
#include "storage/segment/batch_splitter.h"

namespace gvdb::service {

WritePath::WritePath(std::shared_ptr<storage::ISegmentStore> segment_store,
                     std::shared_ptr<compute::QueryExecutor> query_executor,
                     ICollectionResolver& resolver, SegmentLocator& locator,
                     cluster::PrimaryTermTracker* primary_term_tracker,
                     cluster::ShardWriteGate* shard_write_gate)
    : segment_store_(std::move(segment_store)),
      query_executor_(std::move(query_executor)),
      resolver_(resolver),
      locator_(locator),
      primary_term_tracker_(primary_term_tracker),
      shard_write_gate_(shard_write_gate) {}

grpc::Status WritePath::CheckGate(const rpc::PrimaryTermFromHeader& header,
                                  const std::string& collection_name,
                                  uint64_t sample_vector_id) const {
  // Single-node / tests / query-nodes: no tracker plumbed in, gate is
  // a no-op for the primary-term check. The shard-freeze fence is
  // consulted independently: a single-node BackupManager binary can
  // still freeze writes during a snapshot even without a tracker.
  const bool has_tracker = primary_term_tracker_ != nullptr;
  const bool has_freeze_gate = shard_write_gate_ != nullptr;
  if (!has_tracker && !has_freeze_gate) return grpc::Status::OK;

  // Map the request to a shard via the same hash the proxy uses
  // (core::ShardIndexForVector). num_shards comes from the resolver's segment
  // list: every shard owns one segment by the ShardSegmentId scheme.
  // If the resolver can't answer (collection unknown locally), fall
  // through and let the regular handler return its own error.
  auto seg_ids = resolver_.GetSegmentIds(collection_name);
  if (!seg_ids.ok() || seg_ids->empty()) {
    common::Logger::Instance().Debug(
        "Write gate: resolver could not produce segment list for '{}'; "
        "deferring to handler-level error",
        collection_name);
    return grpc::Status::OK;
  }
  const auto num_shards = static_cast<uint32_t>(seg_ids->size());
  const uint32_t shard_id = core::ShardIndexForVector(sample_vector_id, num_shards);

  // Per-shard backup freeze: a frozen shard rejects writes for the
  // duration of an in-flight backup, regardless of primary-term state.
  // Checked BEFORE the primary-term check because a frozen shard's
  // term is fine: we just need writes to back off briefly.
  if (has_freeze_gate &&
      shard_write_gate_->IsFrozen(core::MakeShardId(static_cast<uint16_t>(shard_id)))) {
    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                        absl::StrCat("shard ", shard_id,
                                     " is frozen for backup; retry after the backup completes"));
  }

  if (!has_tracker) return grpc::Status::OK;
  // A client that does not send the header is either an older proxy (rolling
  // upgrade) or a single-node tool that talks directly to the data node.
  // Accept it with a debug log, so a real misconfiguration shows in the
  // metrics instead of breaking the request.
  if (!header.has_term) {
    common::Logger::Instance().Debug("Write to '{}' has no {} header; accepting (back-compat path)",
                                     collection_name, rpc::kPrimaryTermHeader);
    return grpc::Status::OK;
  }
  if (header.parse_error) {
    return grpc::Status(
        grpc::StatusCode::INVALID_ARGUMENT,
        std::string("malformed ") + rpc::kPrimaryTermHeader + " header (must be decimal uint64)");
  }

  using D = cluster::PrimaryTermTracker::AcceptDecision;
  const D decision = primary_term_tracker_->EvaluateWrite(shard_id, header.term);
  switch (decision) {
    case D::Accept:
      return grpc::Status::OK;
    case D::StaleTerm: {
      // The proxy's cached routing has the wrong term. ABORTED tells
      // the proxy to re-route via fresh RouteQuery and retry: the
      // replica-fallback helper classifies ABORTED as transient.
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED,
                          absl::StrCat("stale shard term for shard ", shard_id, " (request term=",
                                       header.term, ", current term=", snap.term, ")"));
    }
    case D::NotPrimary: {
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED,
                          absl::StrCat("not primary for shard ", shard_id,
                                       " (current term=", snap.term, "); re-route via RouteQuery"));
    }
    case D::UnknownShard:
      // Heartbeat hasn't synced yet, OR the proxy's routing genuinely
      // points at a node that doesn't own the shard. Distinct code so
      // observers can tell the two apart. Caller (proxy) should re-
      // route via fresh RouteQuery; we don't tag it ABORTED because
      // the cure is the same but the diagnosis is different.
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                          absl::StrCat("no primary record for shard ", shard_id,
                                       " on this data-node; re-route via RouteQuery"));
  }
  return grpc::Status::OK;
}

grpc::Status WritePath::Insert(const proto::InsertRequest& request, size_t* inserted) {
  // Get all segment IDs for this collection
  auto segment_ids_result = resolver_.GetSegmentIds(request.collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::ToGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;
  size_t num_shards = segment_ids.size();

  // Get dimension from first segment
  auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
  if (!first_segment) {
    return rpc::ToGrpcStatus(absl::NotFoundError("Segment not found"));
  }
  uint32_t dimension = first_segment->GetDimension();
  const core::MetricType metric = first_segment->GetMetric();

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

  for (const auto& proto_vec : request.vectors()) {
    auto vec_result = rpc::FromProto(proto_vec);
    if (!vec_result.ok()) return rpc::ToGrpcStatus(vec_result.status());

    core::VectorId vid = vec_result->first;
    uint32_t shard_idx =
        core::ShardIndexForVector(core::ToUInt64(vid), static_cast<uint32_t>(num_shards));

    auto& batch = shard_batches[shard_idx];
    batch.ids.push_back(vid);
    batch.vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      batch.has_metadata = true;
      auto meta_result = rpc::FromProto(proto_vec.metadata());
      if (!meta_result.ok()) return rpc::ToGrpcStatus(meta_result.status());
      batch.metadata.push_back(std::move(*meta_result));
    } else if (batch.has_metadata) {
      batch.metadata.emplace_back();
    }

    if (proto_vec.has_sparse_vector() && proto_vec.sparse_vector().indices_size() > 0) {
      auto sparse_result = rpc::FromProto(proto_vec.sparse_vector());
      if (!sparse_result.ok()) return rpc::ToGrpcStatus(sparse_result.status());
      batch.has_sparse = true;
      batch.sparse_map[core::ToUInt64(vid)] = std::move(*sparse_result);
    }

    if (proto_vec.ttl_seconds() > 0) {
      batch.expiry_entries[core::ToUInt64(vid)] =
          storage::Segment::NowEpochSeconds() + static_cast<int64_t>(proto_vec.ttl_seconds());
    }
  }

  // Validate dimension
  for (const auto& batch : shard_batches) {
    for (const auto& vec : batch.vectors) {
      if (static_cast<uint32_t>(vec.dimension()) != dimension) {
        return rpc::ToGrpcStatus(absl::InvalidArgumentError(absl::StrFormat(
            "Vector dimension mismatch: expected %d, got %d", dimension, vec.dimension())));
      }
      if (auto status = core::CheckVectorForMetric(metric, vec); !status.ok()) {
        return rpc::ToGrpcStatus(status);
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
      item_costs.push_back(v.byte_size() +
                           (charges_metadata ? storage::Segment::kMetadataBytesEstimate : 0));
    }
    auto ranges_or = storage::SplitBatchBySize(item_costs, max_segment_bytes);
    if (!ranges_or.ok()) {
      common::MetricsRegistry::Instance().RecordInsert(request.collection_name(), false, 0);
      return rpc::ToGrpcStatus(ranges_or.status());
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
        sliced_vectors.assign(batch.vectors.begin() + begin, batch.vectors.begin() + end);
        sliced_ids.assign(batch.ids.begin() + begin, batch.ids.begin() + end);
        if (batch.metadata.size() == batch.ids.size()) {
          sliced_metadata.assign(batch.metadata.begin() + begin, batch.metadata.begin() + end);
        } else {
          // Malformed parallel arrays (metadata missing on leading vectors).
          // Pass as-is so the segment's size-mismatch validation rejects it,
          // matching the unsplit path.
          sliced_metadata = batch.metadata;
        }
        for (const auto& vid : sliced_ids) {
          uint64_t key = core::ToUInt64(vid);
          if (auto it = batch.sparse_map.find(key); it != batch.sparse_map.end()) {
            sliced_sparse.emplace(key, it->second);
          }
          if (auto it = batch.expiry_entries.find(key); it != batch.expiry_entries.end()) {
            sliced_expiry.emplace(key, it->second);
          }
        }
      }
      const auto& range_vectors = single_range ? batch.vectors : sliced_vectors;
      const auto& range_ids = single_range ? batch.ids : sliced_ids;
      const auto& range_metadata = single_range ? batch.metadata : sliced_metadata;
      const auto& range_sparse = single_range ? batch.sparse_map : sliced_sparse;
      const auto& range_expiry = single_range ? batch.expiry_entries : sliced_expiry;

      // Get a writable segment that can fit this range
      auto* segment = segment_store_->GetWritableSegment(collection_id, range_bytes);
      if (!segment) {
        segment = segment_store_->GetSegment(segment_ids[i]);
      }
      if (!segment) break;

      auto write_range = [&](storage::Segment* seg) {
        if (batch.has_sparse) {
          return seg->AddVectorsWithSparse(range_vectors, range_ids, range_metadata, range_sparse,
                                           range_expiry);
        }
        if (batch.has_metadata) {
          return seg->AddVectorsWithMetadata(range_vectors, range_ids, range_metadata,
                                             range_expiry);
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
        common::MetricsRegistry::Instance().RecordInsert(request.collection_name(), false, 0);
        return rpc::ToGrpcStatus(status);
      }

      total_inserted += range_ids.size();
    }
  }

  common::MetricsRegistry::Instance().RecordInsert(request.collection_name(), true, total_inserted);
  *inserted = total_inserted;
  return grpc::Status::OK;
}

grpc::Status WritePath::StreamInsertChunk(const proto::InsertRequest& chunk,
                                          const std::string& metrics_collection,
                                          uint64_t* inserted) {
  auto segment_ids_result = resolver_.GetSegmentIds(chunk.collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::ToGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;
  size_t num_shards = segment_ids.size();

  auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
  if (!first_segment) {
    return rpc::ToGrpcStatus(absl::NotFoundError("Segment not found"));
  }
  uint32_t dimension = first_segment->GetDimension();
  const core::MetricType metric = first_segment->GetMetric();

  struct ShardBatch {
    std::vector<core::Vector> vectors;
    std::vector<core::VectorId> ids;
    std::vector<core::Metadata> metadata;
    bool has_metadata = false;
  };
  std::vector<ShardBatch> shard_batches(num_shards);

  for (const auto& proto_vec : chunk.vectors()) {
    auto vec_result = rpc::FromProto(proto_vec);
    if (!vec_result.ok()) return rpc::ToGrpcStatus(vec_result.status());

    core::VectorId vid = vec_result->first;
    uint32_t shard_idx =
        core::ShardIndexForVector(core::ToUInt64(vid), static_cast<uint32_t>(num_shards));

    auto& batch = shard_batches[shard_idx];
    batch.ids.push_back(vid);
    batch.vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      batch.has_metadata = true;
      auto meta_result = rpc::FromProto(proto_vec.metadata());
      if (!meta_result.ok()) return rpc::ToGrpcStatus(meta_result.status());
      batch.metadata.push_back(std::move(*meta_result));
    } else if (batch.has_metadata) {
      batch.metadata.emplace_back();
    }
  }

  for (const auto& batch : shard_batches) {
    for (const auto& vec : batch.vectors) {
      if (static_cast<uint32_t>(vec.dimension()) != dimension) {
        return rpc::ToGrpcStatus(absl::InvalidArgumentError(absl::StrFormat(
            "Vector dimension mismatch: expected %d, got %d", dimension, vec.dimension())));
      }
      if (auto status = core::CheckVectorForMetric(metric, vec); !status.ok()) {
        return rpc::ToGrpcStatus(status);
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
      common::MetricsRegistry::Instance().RecordInsert(metrics_collection, false, 0);
      return rpc::ToGrpcStatus(status);
    }
    *inserted += batch.ids.size();
  }
  return grpc::Status::OK;
}

grpc::Status WritePath::Upsert(const proto::UpsertRequest& request,
                               storage::Segment::UpsertResult* result) {
  auto segment_ids_result = resolver_.GetSegmentIds(request.collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::ToGrpcStatus(segment_ids_result.status());
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
  vectors.reserve(request.vectors_size());
  ids.reserve(request.vectors_size());
  metadata.reserve(request.vectors_size());

  for (const auto& proto_vec : request.vectors()) {
    auto vec_result = rpc::FromProto(proto_vec);
    if (!vec_result.ok()) {
      return rpc::ToGrpcStatus(vec_result.status());
    }
    ids.push_back(vec_result->first);
    vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      auto meta_result = rpc::FromProto(proto_vec.metadata());
      if (!meta_result.ok()) {
        return rpc::ToGrpcStatus(meta_result.status());
      }
      metadata.push_back(std::move(*meta_result));
    } else {
      metadata.emplace_back();
    }

    if (proto_vec.ttl_seconds() > 0) {
      expiry_entries[core::ToUInt64(vec_result->first)] =
          storage::Segment::NowEpochSeconds() + static_cast<int64_t>(proto_vec.ttl_seconds());
    }
  }

  auto upsert_result = growing_segment->UpsertVectors(vectors, ids, metadata, expiry_entries);
  if (!upsert_result.ok()) {
    return rpc::ToGrpcStatus(upsert_result.status());
  }
  *result = *upsert_result;
  return grpc::Status::OK;
}

grpc::Status WritePath::Delete(const proto::DeleteRequest& request, DeleteOutcome* outcome) {
  // A vector may live in any segment of the collection, so route each id to
  // the segment that actually holds it rather than the resolver's active one.
  auto segments_result = locator_.CollectionSegments(request.collection_name());
  if (!segments_result.ok()) {
    return rpc::ToGrpcStatus(segments_result.status());
  }
  const auto& segments = *segments_result;

  auto remaining =
      request.ids() | std::views::transform(core::MakeVectorId) | std::ranges::to<std::vector>();

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
      return rpc::ToGrpcStatus(result.status());
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
      if (!just_deleted.contains(core::ToUInt64(id))) {
        next.push_back(id);
      }
    }
    remaining = std::move(next);
  }

  outcome->deleted_count = deleted_count;
  outcome->not_found_ids = std::move(remaining);
  return grpc::Status::OK;
}

grpc::Status WritePath::UpdateMetadata(const proto::UpdateMetadataRequest& request,
                                       proto::UpdateMetadataResponse* response) {
  auto segments_result = locator_.CollectionSegments(request.collection_name());
  if (!segments_result.ok()) {
    return rpc::ToGrpcStatus(segments_result.status());
  }
  const auto& segments = *segments_result;

  auto metadata_result = rpc::FromProto(request.metadata());
  if (!metadata_result.ok()) {
    return rpc::ToGrpcStatus(metadata_result.status());
  }

  auto vector_id = core::MakeVectorId(request.id());

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
    auto not_found = absl::NotFoundError(absl::StrCat(
        "Vector ID ", request.id(), " not found in collection '", request.collection_name(), "'"));
    response->set_updated(false);
    response->set_message(std::string(not_found.message()));
    return rpc::ToGrpcStatus(not_found);
  }

  auto status = owner->UpdateMetadata(vector_id, *metadata_result, request.merge());
  if (!status.ok()) {
    response->set_updated(false);
    response->set_message(std::string(status.message()));
    return rpc::ToGrpcStatus(status);
  }
  return grpc::Status::OK;
}

void WritePath::InvalidateCache(const std::string& collection_name) const {
  if (auto* cache = query_executor_->GetCache()) {
    auto cid = resolver_.GetCollectionId(collection_name);
    if (cid.ok()) cache->InvalidateCollection(core::ToUInt32(*cid));
  }
}

}  // namespace gvdb::service

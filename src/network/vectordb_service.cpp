// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/vectordb_service.h"
#include "network/proto_conversions.h"
#include "network/collection_resolver.h"
#include "network/primary_term_header.h"
#include "network/replica_fallback.h"
#include "cluster/primary_term_tracker.h"
#include "cluster/shard_write_gate.h"
#include "cluster/coordinator.h"
#include "auth/auth_context.h"
#include "network/audit_context.h"
#include "storage/backup_manager.h"
#include "storage/batch_splitter.h"
#include "utils/logger.h"
#include "utils/metrics.h"
#include "utils/timer.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "internal.grpc.pb.h"
#include "index/bm25_index.h"
#include "index/sparse_index.h"
#include <algorithm>
#include <chrono>
#include <future>
#include <set>
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

// ============================================================================
// VectorDBService Implementation
// ============================================================================

VectorDBService::VectorDBService(
    std::shared_ptr<storage::ISegmentStore> segment_store,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::unique_ptr<ICollectionResolver> resolver,
    std::shared_ptr<auth::RbacStore> rbac_store,
    std::shared_ptr<storage::BulkImporter> bulk_importer,
    std::shared_ptr<storage::BackupManager> backup_manager,
    std::shared_ptr<storage::RestoreManager> restore_manager)
    : segment_store_(std::move(segment_store)),
      query_executor_(std::move(query_executor)),
      resolver_(std::move(resolver)),
      rbac_store_(std::move(rbac_store)),
      bulk_importer_(std::move(bulk_importer)),
      backup_manager_(std::move(backup_manager)),
      restore_manager_(std::move(restore_manager)) {
  utils::Logger::Instance().Info(
      "VectorDBService initialized (RBAC {}, BulkImport {}, Backup {}, Restore {})",
      rbac_store_ ? "enabled" : "disabled",
      bulk_importer_ ? "enabled" : "disabled",
      backup_manager_ ? "enabled" : "disabled",
      restore_manager_ ? "enabled" : "disabled");
}

VectorDBService::~VectorDBService() = default;

grpc::Status VectorDBService::CheckPermission(
    auth::Permission perm, const std::string& collection_name) const {
  if (!rbac_store_) return grpc::Status::OK;

  const auto& key = auth::AuthContext::GetCurrentKey();
  if (key.empty()) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED,
        "No API key in auth context");
  }

  auto* role = rbac_store_->Lookup(key);
  if (!role) {
    return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Unknown API key");
  }

  if (!auth::HasPermission(role->role, perm)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
        "Role does not have permission for this operation");
  }

  if (!collection_name.empty() && !auth::HasCollectionAccess(*role, collection_name)) {
    return grpc::Status(grpc::StatusCode::PERMISSION_DENIED,
        "Access denied for collection: " + collection_name);
  }

  return grpc::Status::OK;
}

// ============================================================================
// Helpers
// ============================================================================

grpc::Status VectorDBService::EvaluateWriteGate(
    grpc::ServerContext* context, const std::string& collection_name,
    uint64_t sample_vector_id) {
  // Single-node / tests / query-nodes: no tracker plumbed in, gate is
  // a no-op for the primary-term check. The shard-freeze fence is
  // consulted independently — a single-node BackupManager binary can
  // still freeze writes during a snapshot even without a tracker.
  const bool has_tracker = primary_term_tracker_ != nullptr;
  const bool has_freeze_gate = shard_write_gate_ != nullptr;
  if (!has_tracker && !has_freeze_gate) return grpc::Status::OK;

  // Read the per-write term from the gRPC client metadata header. A
  // client that doesn't send the header is either a pre-1.x proxy
  // (rolling upgrade) or a single-node tool talking directly to the
  // data-node. Accept silently with a debug log so a real misconfig
  // is still observable in operator dashboards via metrics, not by
  // breaking the request.
  auto hdr = ReadPrimaryTermHeader(context);

  // Map the request to a shard via the same hash the proxy uses
  // (vid % num_shards). num_shards comes from the resolver's segment
  // list — every shard owns one segment by the ShardSegmentId scheme.
  // If the resolver can't answer (collection unknown locally), fall
  // through and let the regular handler return its own error.
  auto seg_ids = resolver_->GetSegmentIds(collection_name);
  if (!seg_ids.ok() || seg_ids->empty()) {
    utils::Logger::Instance().Debug(
        "Write gate: resolver could not produce segment list for '{}'; "
        "deferring to handler-level error", collection_name);
    return grpc::Status::OK;
  }
  const uint32_t num_shards = static_cast<uint32_t>(seg_ids->size());
  const uint32_t shard_id =
      static_cast<uint32_t>(sample_vector_id % num_shards);

  // Per-shard backup freeze: a frozen shard rejects writes for the
  // duration of an in-flight backup, regardless of primary-term state.
  // Checked BEFORE the primary-term check because a frozen shard's
  // term is fine — we just need writes to back off briefly.
  if (has_freeze_gate &&
      shard_write_gate_->IsFrozen(core::MakeShardId(
          static_cast<uint16_t>(shard_id)))) {
    return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, absl::StrCat(
        "shard ", shard_id,
        " is frozen for backup; retry after the backup completes"));
  }

  if (!has_tracker) return grpc::Status::OK;
  if (!hdr.has_term) {
    utils::Logger::Instance().Debug(
        "Write to '{}' has no {} header; accepting (back-compat path)",
        collection_name, kPrimaryTermHeader);
    return grpc::Status::OK;
  }
  if (hdr.parse_error) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        std::string("malformed ") + kPrimaryTermHeader +
        " header (must be decimal uint64)");
  }

  using D = cluster::PrimaryTermTracker::AcceptDecision;
  const D decision = primary_term_tracker_->EvaluateWrite(shard_id, hdr.term);
  switch (decision) {
    case D::Accept:
      return grpc::Status::OK;
    case D::StaleTerm: {
      // The proxy's cached routing has the wrong term. ABORTED tells
      // the proxy to re-route via fresh RouteQuery and retry — the
      // replica-fallback helper classifies ABORTED as transient.
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED, absl::StrCat(
          "stale shard term for shard ", shard_id,
          " (request term=", hdr.term, ", current term=", snap.term, ")"));
    }
    case D::NotPrimary: {
      auto snap = primary_term_tracker_->Get(shard_id);
      return grpc::Status(grpc::StatusCode::ABORTED, absl::StrCat(
          "not primary for shard ", shard_id,
          " (current term=", snap.term, "); re-route via RouteQuery"));
    }
    case D::UnknownShard:
      // Heartbeat hasn't synced yet, OR the proxy's routing genuinely
      // points at a node that doesn't own the shard. Distinct code so
      // observers can tell the two apart. Caller (proxy) should re-
      // route via fresh RouteQuery; we don't tag it ABORTED because
      // the cure is the same but the diagnosis is different.
      return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, absl::StrCat(
          "no primary record for shard ", shard_id,
          " on this data-node; re-route via RouteQuery"));
  }
  return grpc::Status::OK;
}

storage::Segment* VectorDBService::GetOrReplicateSegment(core::SegmentId segment_id) {
  auto* segment = segment_store_->GetSegment(segment_id);
  if (segment) return segment;

  // Segment not found locally — try pulling from coordinator if available
  auto* stub = resolver_->GetCoordinatorStub();
  if (!stub) return nullptr;

  utils::Logger::Instance().Info("Segment {} not found locally, pulling from coordinator",
                                 static_cast<uint64_t>(core::ToUInt32(segment_id)));

  grpc::ClientContext context;
  proto::internal::GetSegmentRequest request;
  request.set_segment_id(static_cast<uint64_t>(core::ToUInt32(segment_id)));
  proto::internal::GetSegmentResponse response;

  auto grpc_status = stub->GetSegment(&context, request, &response);
  if (!grpc_status.ok()) {
    utils::Logger::Instance().Error("Failed to fetch segment {} from coordinator: {}",
                                     static_cast<uint64_t>(core::ToUInt32(segment_id)),
                                     grpc_status.error_message());
    return nullptr;
  }

  if (!response.has_segment_info() || response.segment_data().empty()) {
    utils::Logger::Instance().Warn("Segment {} not found on coordinator",
                                   static_cast<uint64_t>(core::ToUInt32(segment_id)));
    return nullptr;
  }

  auto segment_result = storage::Segment::DeserializeFromBytes(response.segment_data());
  if (!segment_result.ok()) {
    utils::Logger::Instance().Error("Failed to deserialize segment {}: {}",
                                     response.segment_info().segment_id(),
                                     segment_result.status().message());
    return nullptr;
  }

  auto add_status = segment_store_->AddReplicatedSegment(std::move(segment_result.value()));
  if (!add_status.ok()) {
    utils::Logger::Instance().Error("Failed to add replicated segment {}: {}",
                                     response.segment_info().segment_id(),
                                     add_status.message());
    return nullptr;
  }

  utils::Logger::Instance().Info("Replicated segment {} ({} vectors)",
                                 response.segment_info().segment_id(),
                                 response.segment_info().vector_count());

  return segment_store_->GetSegment(segment_id);
}

grpc::Status VectorDBService::SearchDistributed(
    const proto::SearchRequest* request,
    proto::SearchResponse* response,
    const core::Vector& query) {

  utils::Timer timer;

  // Get shard targets from coordinator
  auto targets_result = resolver_->GetShardTargets(request->collection_name());
  if (!targets_result.ok()) {
    return toGrpcStatus(targets_result.status());
  }

  const auto& targets = *targets_result;
  if (targets.empty()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
        "No shards found for collection: " + request->collection_name());
  }

  utils::Logger::Instance().Info("Distributed search: {} shards for '{}'",
                                  targets.size(), request->collection_name());

  // Fan out ExecuteShardQuery to each data node in parallel. Each shard
  // walks its own candidate list (primary + routable replicas) on
  // transient failures, so a draining or briefly-unreachable primary
  // doesn't break the whole search.
  struct ShardEntry {
    uint64_t id;
    float distance;
    proto::Metadata metadata;
  };
  struct ShardResult {
    uint32_t shard_id = 0;
    std::vector<ShardEntry> entries;
    bool ok = false;
    grpc::StatusCode last_status_code = grpc::StatusCode::OK;
    std::string last_error;
  };

  std::vector<std::future<ShardResult>> futures;
  futures.reserve(targets.size());

  for (const auto& target : targets) {
    futures.push_back(std::async(std::launch::async,
        [&target, &query, &request]() -> ShardResult {
          ShardResult result;
          result.shard_id = target.shard_id;

          if (target.options.empty()) {
            // Coordinator returned no candidates for this shard at all;
            // surface explicitly rather than silently dropping.
            result.last_status_code = grpc::StatusCode::FAILED_PRECONDITION;
            result.last_error = "no routable nodes for shard";
            return result;
          }

          // Project resolver-side options into the proto type the helper
          // consumes. Same data, different message — the resolver layer
          // intentionally keeps internal.pb.h out of its public header.
          google::protobuf::RepeatedPtrField<
              proto::internal::RouteQueryNodeOption>
              proto_options;
          for (const auto& o : target.options) {
            auto* p = proto_options.Add();
            p->set_node_id(o.node_id);
            p->set_node_address(o.node_address);
            p->set_is_primary(o.is_primary);
          }

          auto rpc_result = CallWithReplicaFallback(
              proto_options, std::chrono::milliseconds(10000), "search",
              [&](grpc::ClientContext* ctx,
                  const std::string& addr) -> grpc::Status {
                // Defensive clear: today's helper returns on first OK,
                // so this lambda runs at most once with a successful
                // status. If a future maintainer changes the helper's
                // strategy (e.g. best-of-N) this prevents accumulating
                // duplicate entries from multiple successful retries.
                result.entries.clear();

                auto channel = grpc::CreateChannel(
                    addr, grpc::InsecureChannelCredentials());
                auto stub =
                    proto::internal::InternalService::NewStub(channel);

                proto::internal::ExecuteShardQueryRequest shard_req;
                shard_req.set_collection_id(target.collection_id);
                shard_req.set_shard_id(target.shard_id);
                shard_req.set_top_k(request->top_k());
                shard_req.set_filter(request->filter());
                shard_req.set_return_metadata(request->return_metadata());
                for (int i = 0; i < query.dimension(); ++i) {
                  shard_req.add_query_vector(query.data()[i]);
                }

                proto::internal::ExecuteShardQueryResponse shard_resp;
                auto status = stub->ExecuteShardQuery(ctx, shard_req,
                                                       &shard_resp);
                if (status.ok()) {
                  for (const auto& r : shard_resp.results()) {
                    ShardEntry entry;
                    entry.id = r.id();
                    entry.distance = r.distance();
                    if (r.metadata().fields_size() > 0) {
                      entry.metadata = r.metadata();
                    }
                    result.entries.push_back(std::move(entry));
                  }
                }
                return status;
              });

          result.ok = rpc_result.final_status.ok();
          result.last_status_code = rpc_result.final_status.error_code();
          result.last_error =
              std::string(rpc_result.final_status.error_message());
          if (!result.ok) {
            utils::Logger::Instance().Warn(
                "Shard {} search exhausted candidates: {}",
                target.shard_id, result.last_error);
          }
          return result;
        }));
  }

  // Collect and merge results. Surface partial failures explicitly:
  // silently dropping a shard's contribution would return a successful
  // but truncated top-K to the caller, masking real availability loss.
  std::vector<ShardEntry> all_entries;
  std::vector<uint32_t> failed_shards;
  std::string last_error_summary;
  grpc::StatusCode last_failed_code = grpc::StatusCode::UNAVAILABLE;
  for (auto& future : futures) {
    auto result = future.get();
    if (result.ok) {
      for (auto& e : result.entries) {
        all_entries.push_back(std::move(e));
      }
    } else {
      failed_shards.push_back(result.shard_id);
      last_failed_code = result.last_status_code;
      last_error_summary = result.last_error;
    }
  }
  if (!failed_shards.empty()) {
    std::string ids;
    for (size_t i = 0; i < failed_shards.size(); ++i) {
      if (i > 0) ids += ",";
      ids += std::to_string(failed_shards[i]);
    }
    return grpc::Status(
        last_failed_code,
        absl::StrCat("distributed search incomplete: shards [", ids,
                      "] exhausted all candidates (last error: ",
                      last_error_summary, ")"));
  }

  // Sort by distance ascending (L2/cosine) and take top_k
  std::sort(all_entries.begin(), all_entries.end(),
            [](const auto& a, const auto& b) { return a.distance < b.distance; });

  int top_k = std::min(static_cast<int>(all_entries.size()),
                        static_cast<int>(request->top_k()));

  for (int i = 0; i < top_k; ++i) {
    auto* proto_entry = response->add_results();
    proto_entry->set_id(all_entries[i].id);
    proto_entry->set_distance(all_entries[i].distance);

    if (request->return_metadata() && all_entries[i].metadata.fields_size() > 0) {
      *proto_entry->mutable_metadata() = all_entries[i].metadata;
    }
  }

  response->set_query_time_ms(timer.elapsed_millis());

  total_queries_.fetch_add(1, std::memory_order_relaxed);
  total_query_time_ms_.fetch_add(
      static_cast<uint64_t>(timer.elapsed_millis()),
      std::memory_order_relaxed);

  return grpc::Status::OK;
}

// ============================================================================
// Collection Management
// ============================================================================

grpc::Status VectorDBService::CreateCollection(
    grpc::ServerContext* context,
    const proto::CreateCollectionRequest* request,
    proto::CreateCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::CREATE_COLLECTION, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());

  utils::Logger::Instance().Info("CreateCollection: {}", request->collection_name());

  constexpr uint32_t MAX_DIMENSION = 8192; // TODO: remove this fixed value.
  constexpr uint32_t MIN_DIMENSION = 1;

  if (request->dimension() < MIN_DIMENSION || request->dimension() > MAX_DIMENSION) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Dimension must be between ", MIN_DIMENSION, " and ",
                     MAX_DIMENSION, ". Requested: ", request->dimension()));
  }

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Collection name cannot be empty");
  }

  auto metric_result = fromProto(request->metric());
  if (!metric_result.ok()) return toGrpcStatus(metric_result.status());

  auto index_type_result = fromProto(request->index_type());
  if (!index_type_result.ok()) return toGrpcStatus(index_type_result.status());

  size_t num_shards = request->num_shards() > 0 ? request->num_shards() : 1;
  auto collection_id_result = resolver_->CreateCollection(
      request->collection_name(), request->dimension(),
      *metric_result, *index_type_result, num_shards);

  if (!collection_id_result.ok()) {
    return toGrpcStatus(collection_id_result.status());
  }

  response->set_collection_id(core::ToUInt32(*collection_id_result));
  response->set_message("Collection created successfully");

  utils::MetricsRegistry::Instance().SetCollectionCount(
      resolver_->CollectionCount());

  return grpc::Status::OK;
}

grpc::Status VectorDBService::DropCollection(
    grpc::ServerContext* context,
    const proto::DropCollectionRequest* request,
    proto::DropCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::DROP_COLLECTION, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());

  utils::Logger::Instance().Info("DropCollection: {}", request->collection_name());

  // Get collection ID before drop (for cache invalidation)
  auto cid_before = resolver_->GetCollectionId(request->collection_name());

  auto status = resolver_->DropCollection(request->collection_name());
  if (!status.ok()) return toGrpcStatus(status);

  utils::MetricsRegistry::Instance().SetCollectionCount(
      resolver_->CollectionCount());

  // Invalidate query cache
  if (auto* cache = query_executor_->GetCache()) {
    if (cid_before.ok()) cache->InvalidateCollection(core::ToUInt32(*cid_before));
  }

  response->set_message("Collection dropped successfully");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListCollections(
    grpc::ServerContext* context,
    const proto::ListCollectionsRequest* request,
    proto::ListCollectionsResponse* response) {
  auto perm = CheckPermission(auth::Permission::LIST_COLLECTIONS, "");
  if (!perm.ok()) return perm;

  auto collections = resolver_->ListCollections();

  for (const auto& coll : collections) {
    auto* info = response->add_collections();
    info->set_collection_id(core::ToUInt32(coll.collection_id));
    info->set_collection_name(coll.collection_name);
    info->set_dimension(coll.dimension);
    info->set_metric_type(toString(coll.metric_type));
    info->set_vector_count(coll.vector_count);
  }

  return grpc::Status::OK;
}

// ============================================================================
// Vector Operations
// ============================================================================

grpc::Status VectorDBService::Insert(
    grpc::ServerContext* context,
    const proto::InsertRequest* request,
    proto::InsertResponse* response) {
  auto perm = CheckPermission(auth::Permission::INSERT, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->vectors().size());

  utils::Logger::Instance().Info("Insert: {} vectors into {}",
                                 request->vectors().size(),
                                 request->collection_name());

  utils::MetricsRegistry::Instance().RecordBatchSize(request->vectors().size());

  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::INSERT,
      request->collection_name());

  constexpr size_t MAX_BATCH_SIZE = 50000;
  constexpr uint32_t MAX_DIMENSION = 8192;

  if (request->vectors().size() > MAX_BATCH_SIZE) {
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

  // Primary-term gate. Use the first vector's id as the shard sample
  // — the proxy splits per-shard before sending so all vectors in an
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
    return toGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;
  size_t num_shards = segment_ids.size();

  // Get dimension from first segment
  auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
  if (!first_segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
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
    auto vec_result = fromProto(proto_vec);
    if (!vec_result.ok()) return toGrpcStatus(vec_result.status());

    core::VectorId vid = vec_result->first;
    uint32_t shard_idx = static_cast<uint32_t>(
        core::ToUInt64(vid) % num_shards);

    auto& batch = shard_batches[shard_idx];
    batch.ids.push_back(vid);
    batch.vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      batch.has_metadata = true;
      auto meta_result = fromProto(proto_vec.metadata());
      if (!meta_result.ok()) return toGrpcStatus(meta_result.status());
      batch.metadata.push_back(std::move(*meta_result));
    } else if (batch.has_metadata) {
      batch.metadata.push_back(core::Metadata{});
    }

    if (proto_vec.has_sparse_vector() && proto_vec.sparse_vector().indices_size() > 0) {
      auto sparse_result = fromProto(proto_vec.sparse_vector());
      if (!sparse_result.ok()) return toGrpcStatus(sparse_result.status());
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
      if (vec.dimension() != dimension) {
        return toGrpcStatus(absl::InvalidArgumentError(
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
      utils::MetricsRegistry::Instance().RecordInsert(
          request->collection_name(), false, 0);
      return toGrpcStatus(ranges_or.status());
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
        utils::MetricsRegistry::Instance().RecordInsert(
            request->collection_name(), false, 0);
        return toGrpcStatus(status);
      }

      total_inserted += range_ids.size();
    }
  }

  utils::MetricsRegistry::Instance().RecordInsert(
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
    grpc::ServerContext* context,
    grpc::ServerReader<proto::InsertRequest>* reader,
    proto::InsertResponse* response) {

  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::INSERT,
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
      AuditContext::SetCollection(collection_name);
      perm_checked = true;
    }

    auto segment_ids_result = resolver_->GetSegmentIds(chunk.collection_name());
    if (!segment_ids_result.ok()) {
      return toGrpcStatus(segment_ids_result.status());
    }
    const auto& segment_ids = *segment_ids_result;
    size_t num_shards = segment_ids.size();

    auto* first_segment = segment_store_->GetSegment(segment_ids[0]);
    if (!first_segment) {
      return toGrpcStatus(absl::NotFoundError("Segment not found"));
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
      auto vec_result = fromProto(proto_vec);
      if (!vec_result.ok()) return toGrpcStatus(vec_result.status());

      core::VectorId vid = vec_result->first;
      uint32_t shard_idx = static_cast<uint32_t>(
          core::ToUInt64(vid) % num_shards);

      auto& batch = shard_batches[shard_idx];
      batch.ids.push_back(vid);
      batch.vectors.push_back(std::move(vec_result->second));

      if (proto_vec.has_metadata()) {
        batch.has_metadata = true;
        auto meta_result = fromProto(proto_vec.metadata());
        if (!meta_result.ok()) return toGrpcStatus(meta_result.status());
        batch.metadata.push_back(std::move(*meta_result));
      } else if (batch.has_metadata) {
        batch.metadata.push_back(core::Metadata{});
      }
    }

    for (const auto& batch : shard_batches) {
      for (const auto& vec : batch.vectors) {
        if (vec.dimension() != dimension) {
          return toGrpcStatus(absl::InvalidArgumentError(
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
        utils::MetricsRegistry::Instance().RecordInsert(collection_name, false, 0);
        return toGrpcStatus(status);
      }
      total_inserted += batch.ids.size();
    }
  }

  utils::MetricsRegistry::Instance().RecordInsert(
      collection_name, true, total_inserted);
  utils::MetricsRegistry::Instance().RecordBatchSize(total_inserted);

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
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->vectors().size());
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
    return toGrpcStatus(segment_ids_result.status());
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
    auto vec_result = fromProto(proto_vec);
    if (!vec_result.ok()) {
      return toGrpcStatus(vec_result.status());
    }
    ids.push_back(vec_result->first);
    vectors.push_back(std::move(vec_result->second));

    if (proto_vec.has_metadata()) {
      auto meta_result = fromProto(proto_vec.metadata());
      if (!meta_result.ok()) {
        return toGrpcStatus(meta_result.status());
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
    return toGrpcStatus(upsert_result.status());
  }

  response->set_upserted_count(upsert_result->inserted_count + upsert_result->updated_count);
  response->set_inserted_count(upsert_result->inserted_count);
  response->set_updated_count(upsert_result->updated_count);
  response->set_message("Upsert completed");

  utils::Logger::Instance().Debug("Upsert: {} inserted, {} updated in collection '{}'",
      upsert_result->inserted_count, upsert_result->updated_count,
      request->collection_name());

  return grpc::Status::OK;
}

// ============================================================================
// Range Search
// ============================================================================

grpc::Status VectorDBService::RangeSearch(
    grpc::ServerContext* context,
    const proto::RangeSearchRequest* request,
    proto::RangeSearchResponse* response) {
  auto perm = CheckPermission(auth::Permission::RANGE_SEARCH, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());
  utils::Timer timer;

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Collection name is required");
  }
  if (!request->has_query_vector()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Query vector is required");
  }
  if (request->radius() <= 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Radius must be positive");
  }

  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());

  // Check if we have local segments; if not, fan out to data nodes
  bool any_local = false;
  if (segment_ids_result.ok()) {
    for (const auto& seg_id : *segment_ids_result) {
      if (segment_store_->GetSegment(seg_id)) { any_local = true; break; }
    }
  }

  if (!any_local) {
    auto targets = resolver_->GetShardTargets(request->collection_name());
    if (targets.ok() && !targets->empty()) {
      return RangeSearchDistributed(request, response);
    }
    if (!segment_ids_result.ok()) {
      return toGrpcStatus(segment_ids_result.status());
    }
  }

  const auto& segment_ids = *segment_ids_result;

  auto query_result = fromProto(request->query_vector());
  if (!query_result.ok()) {
    return toGrpcStatus(query_result.status());
  }

  int max_results = request->max_results() > 0 ? request->max_results() : 1000;

  // Search each segment and merge results
  std::vector<core::SearchResultEntry> all_results;
  for (auto seg_id : segment_ids) {
    auto* segment = GetOrReplicateSegment(seg_id);
    if (!segment) continue;

    core::StatusOr<core::SearchResult> seg_result;
    if (request->filter().empty()) {
      seg_result = segment->SearchRange(*query_result, request->radius(), max_results);
    } else {
      seg_result = segment->SearchRangeWithFilter(
          *query_result, request->radius(), request->filter(), max_results);
    }

    if (seg_result.ok()) {
      all_results.insert(all_results.end(),
                         seg_result->entries.begin(), seg_result->entries.end());
    }
  }

  // Sort by distance and limit
  std::sort(all_results.begin(), all_results.end(),
            [](const auto& a, const auto& b) { return a.distance < b.distance; });
  if (static_cast<int>(all_results.size()) > max_results) {
    all_results.resize(max_results);
  }

  // Build response
  for (const auto& entry : all_results) {
    auto* proto_entry = response->add_results();
    toProto(entry, proto_entry);

    if (request->return_metadata()) {
      for (auto seg_id : segment_ids) {
        auto* segment = segment_store_->GetSegment(seg_id);
        if (segment) {
          auto meta = segment->GetMetadata(entry.id);
          if (meta.ok()) {
            toProto(*meta, proto_entry->mutable_metadata());
            break;
          }
        }
      }
    }
  }

  response->set_query_time_ms(static_cast<float>(timer.elapsed_millis()));
  total_queries_++;

  return grpc::Status::OK;
}

grpc::Status VectorDBService::RangeSearchDistributed(
    const proto::RangeSearchRequest* request,
    proto::RangeSearchResponse* response) {

  utils::Timer timer;

  auto targets_result = resolver_->GetShardTargets(request->collection_name());
  if (!targets_result.ok()) {
    return toGrpcStatus(targets_result.status());
  }

  const auto& targets = *targets_result;
  if (targets.empty()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
        "No shards found for collection: " + request->collection_name());
  }

  // Deduplicate node addresses — multiple shards may live on the same
  // node, in which case one RangeSearch RPC covers all of them. We do
  // NOT use replica fallback at the per-node level here: RangeSearch is
  // collection-scoped (no shard selector), so a "replica" of one shard
  // may not host the other shards the original node owned. Falling over
  // to such a replica would return a successful-but-truncated response
  // — re-introducing the silent partial-truncation hazard this PR
  // explicitly fixes elsewhere. Per-shard fallback for RangeSearch
  // requires a shard-scoped RangeSearch RPC and is intentionally
  // deferred; until then, a single-node failure surfaces UNAVAILABLE
  // listing the failed node — visible, not silent.
  std::set<std::string> node_addrs;
  for (const auto& t : targets) {
    if (!t.node_address.empty()) node_addrs.insert(t.node_address);
  }

  struct NodeResult {
    std::string addr;
    proto::RangeSearchResponse resp;
    bool ok = false;
    grpc::StatusCode last_status_code = grpc::StatusCode::OK;
    std::string last_error;
  };

  std::vector<std::future<NodeResult>> futures;
  futures.reserve(node_addrs.size());

  for (const auto& addr : node_addrs) {
    futures.push_back(std::async(std::launch::async,
        [addr, &request]() -> NodeResult {
          NodeResult result;
          result.addr = addr;

          auto channel = grpc::CreateChannel(
              addr, grpc::InsecureChannelCredentials());
          auto stub = proto::VectorDBService::NewStub(channel);

          grpc::ClientContext ctx;
          ctx.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(10));

          auto status = stub->RangeSearch(&ctx, *request, &result.resp);
          result.ok = status.ok();
          result.last_status_code = status.error_code();
          result.last_error = std::string(status.error_message());
          if (!result.ok) {
            utils::Logger::Instance().Warn(
                "Distributed RangeSearch failed on {}: {}",
                result.addr, result.last_error);
          }
          return result;
        }));
  }

  // Collect and merge results. Surface partial failures explicitly so a
  // caller never gets a successful-looking response that omits a node's
  // contribution.
  int max_results = request->max_results() > 0 ? request->max_results() : 1000;
  std::vector<proto::SearchResultEntry> all_entries;
  std::vector<std::string> failed_nodes;
  std::string last_error_summary;
  grpc::StatusCode last_failed_code = grpc::StatusCode::UNAVAILABLE;

  for (auto& future : futures) {
    auto result = future.get();
    if (result.ok) {
      for (const auto& entry : result.resp.results()) {
        all_entries.push_back(entry);
      }
    } else {
      failed_nodes.push_back(result.addr);
      last_failed_code = result.last_status_code;
      last_error_summary = result.last_error;
    }
  }
  if (!failed_nodes.empty()) {
    std::string addrs;
    for (size_t i = 0; i < failed_nodes.size(); ++i) {
      if (i > 0) addrs += ",";
      addrs += failed_nodes[i];
    }
    return grpc::Status(
        last_failed_code,
        absl::StrCat("distributed range search incomplete: nodes [", addrs,
                      "] exhausted all candidates (last error: ",
                      last_error_summary, ")"));
  }

  // Sort by distance and limit
  std::sort(all_entries.begin(), all_entries.end(),
            [](const auto& a, const auto& b) {
              return a.distance() < b.distance();
            });
  if (static_cast<int>(all_entries.size()) > max_results) {
    all_entries.resize(max_results);
  }

  for (auto& entry : all_entries) {
    *response->add_results() = std::move(entry);
  }

  response->set_query_time_ms(static_cast<float>(timer.elapsed_millis()));
  total_queries_++;

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Search(
    grpc::ServerContext* context,
    const proto::SearchRequest* request,
    proto::SearchResponse* response) {
  auto perm = CheckPermission(auth::Permission::SEARCH, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());

  utils::Timer search_timer;

  utils::MetricsTimer metrics_timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::SEARCH,
      request->collection_name());

  // Get collection ID
  auto collection_id_result = resolver_->GetCollectionId(request->collection_name());
  if (!collection_id_result.ok()) {
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(collection_id_result.status());
  }

  // Convert query vector
  auto query_result = fromProto(request->query_vector());
  if (!query_result.ok()) {
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(query_result.status());
  }

  // Get all segments for this collection
  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    // Try distributed search
    auto targets_result = resolver_->GetShardTargets(request->collection_name());
    if (targets_result.ok() && !targets_result->empty()) {
      utils::MetricsRegistry::Instance().RecordSearch(
          request->collection_name(), true);
      return SearchDistributed(request, response, *query_result);
    }
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(segment_ids_result.status());
  }

  const auto& segment_ids = *segment_ids_result;

  // Check if any segment exists locally
  bool any_local = false;
  for (const auto& seg_id : segment_ids) {
    if (segment_store_->GetSegment(seg_id)) { any_local = true; break; }
  }

  if (!any_local) {
    // No local segments — try distributed search
    auto targets_result = resolver_->GetShardTargets(request->collection_name());
    if (targets_result.ok() && !targets_result->empty()) {
      utils::MetricsRegistry::Instance().RecordSearch(
          request->collection_name(), true);
      return SearchDistributed(request, response, *query_result);
    }
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(absl::NotFoundError("No segments found"));
  }

  // Delegate search to QueryExecutor (handles caching, filtering, multi-segment merge)
  core::CollectionId collection_id = *collection_id_result;
  auto search_result = query_executor_->Search(
      collection_id, *query_result, request->top_k(), request->filter());

  if (!search_result.ok()) {
    utils::MetricsRegistry::Instance().RecordSearch(
        request->collection_name(), false);
    return toGrpcStatus(search_result.status());
  }

  utils::MetricsRegistry::Instance().RecordSearch(
      request->collection_name(), true);

  // Build response with optional metadata enrichment
  for (const auto& entry : search_result->entries) {
    auto* proto_entry = response->add_results();
    toProto(entry, proto_entry);

    if (request->return_metadata()) {
      for (const auto& seg_id : segment_ids) {
        auto* segment = segment_store_->GetSegment(seg_id);
        if (segment) {
          auto meta_result = segment->GetMetadata(entry.id);
          if (meta_result.ok()) {
            toProto(*meta_result, proto_entry->mutable_metadata());
            break;
          }
        }
      }
    }
  }

  response->set_query_time_ms(search_timer.elapsed_millis());

  total_queries_.fetch_add(1, std::memory_order_relaxed);
  total_query_time_ms_.fetch_add(
      static_cast<uint64_t>(search_timer.elapsed_millis()),
      std::memory_order_relaxed);

  return grpc::Status::OK;
}

grpc::Status VectorDBService::HybridSearch(
    grpc::ServerContext* context,
    const proto::HybridSearchRequest* request,
    proto::HybridSearchResponse* response) {
  auto perm = CheckPermission(auth::Permission::HYBRID_SEARCH, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());

  utils::Timer search_timer;
  utils::Logger::Instance().Info("HybridSearch: collection={}, text_query='{}'",
                                  request->collection_name(), request->text_query());

  bool has_vector = request->has_query_vector() && request->query_vector().dimension() > 0;
  bool has_text = !request->text_query().empty();
  bool has_sparse = request->has_sparse_query() && request->sparse_query().indices_size() > 0;

  if (!has_vector && !has_text && !has_sparse) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "At least one of query_vector, text_query, or sparse_query must be provided");
  }

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "HybridSearch not supported on coordinator nodes");
  }

  // Resolve collection
  auto collection_id_result = resolver_->GetCollectionId(request->collection_name());
  if (!collection_id_result.ok()) {
    return toGrpcStatus(collection_id_result.status());
  }

  // Convert query vector (optional — may be text-only)
  std::optional<core::Vector> query_vector;
  if (has_vector) {
    auto qr = fromProto(request->query_vector());
    if (!qr.ok()) return toGrpcStatus(qr.status());
    query_vector = std::move(*qr);
  }

  // Convert sparse query (optional)
  std::optional<core::SparseVector> sparse_query;
  if (has_sparse) {
    auto sq = fromProto(request->sparse_query());
    if (!sq.ok()) return toGrpcStatus(sq.status());
    sparse_query = std::move(*sq);
  }
  float sparse_weight = request->sparse_weight() > 0 ? request->sparse_weight() : 0.0f;

  // Get segments
  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    return toGrpcStatus(segment_ids_result.status());
  }

  // Determine weights
  float vector_weight = request->vector_weight() > 0 ? request->vector_weight() : 0.5f;
  float text_weight = request->text_weight() > 0 ? request->text_weight() : 0.5f;
  std::string text_field = request->text_field().empty() ? "text" : request->text_field();

  // Build text index on-the-fly for segments that don't have one
  for (const auto& seg_id : *segment_ids_result) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;

    // Build text index from metadata if not already built
    auto bm25 = std::make_unique<index::BM25Index>();
    segment->BuildTextIndex(std::move(bm25), text_field);

    // Build sparse index from stored sparse vectors if query needs it
    if (has_sparse) {
      auto sparse_idx = std::make_unique<index::SparseIndex>();
      segment->BuildSparseIndex(std::move(sparse_idx));
    }
  }

  // Search each segment
  std::vector<core::SearchResultEntry> all_entries;
  for (const auto& seg_id : *segment_ids_result) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;

    core::StatusOr<core::SearchResult> result;

    const core::SparseVector* sq_ptr = sparse_query ? &*sparse_query : nullptr;

    if (has_vector || has_text || has_sparse) {
      // Hybrid: any combination of vector + text + sparse with RRF fusion
      core::Vector qv = has_vector
          ? *query_vector
          : core::ZeroVector(segment->GetDimension());
      float vw = has_vector ? vector_weight : 0.0f;
      float tw = has_text ? text_weight : 0.0f;
      float sw = has_sparse ? sparse_weight : 0.0f;

      result = segment->SearchHybrid(
          qv, request->text_query(), request->top_k(),
          vw, tw, text_field, sq_ptr, sw);
    }

    if (result.ok()) {
      for (auto& entry : result->entries) {
        all_entries.push_back(entry);
      }
    }
  }

  // Sort by RRF score (higher = better) and take top_k
  std::sort(all_entries.begin(), all_entries.end(),
            [](const auto& a, const auto& b) { return a.distance > b.distance; });

  int top_k = std::min(static_cast<int>(all_entries.size()),
                        static_cast<int>(request->top_k()));

  for (int i = 0; i < top_k; ++i) {
    auto* proto_entry = response->add_results();
    toProto(all_entries[i], proto_entry);

    if (request->return_metadata()) {
      for (const auto& seg_id : *segment_ids_result) {
        auto* segment = segment_store_->GetSegment(seg_id);
        if (segment) {
          auto meta_result = segment->GetMetadata(all_entries[i].id);
          if (meta_result.ok()) {
            toProto(*meta_result, proto_entry->mutable_metadata());
            break;
          }
        }
      }
    }
  }

  response->set_query_time_ms(search_timer.elapsed_millis());
  return grpc::Status::OK;
}

grpc::Status VectorDBService::Get(
    grpc::ServerContext* context,
    const proto::GetRequest* request,
    proto::GetResponse* response) {
  auto perm = CheckPermission(auth::Permission::GET, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->ids().size());

  utils::Logger::Instance().Info("Get: {} IDs from {}",
                                 request->ids().size(),
                                 request->collection_name());

  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::GET,
      request->collection_name());

  if (request->ids().empty()) {
    utils::MetricsRegistry::Instance().RecordGet(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "IDs list cannot be empty");
  }

  constexpr size_t MAX_GET_BATCH_SIZE = 10000;
  if (request->ids().size() > MAX_GET_BATCH_SIZE) {
    utils::MetricsRegistry::Instance().RecordGet(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Cannot get more than ", MAX_GET_BATCH_SIZE,
                     " vectors in one request. Requested: ", request->ids().size()));
  }

  if (!resolver_->SupportsDataOps()) {
    utils::MetricsRegistry::Instance().RecordGet(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Get operations not supported on coordinator nodes. "
        "Send get requests to data nodes instead.");
  }

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    utils::MetricsRegistry::Instance().RecordGet(
        request->collection_name(), false);
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = GetOrReplicateSegment(*segment_id_result);
  if (!segment) {
    utils::MetricsRegistry::Instance().RecordGet(
        request->collection_name(), false);
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  std::vector<core::VectorId> ids;
  ids.reserve(request->ids().size());
  for (uint64_t id : request->ids()) {
    ids.push_back(core::MakeVectorId(id));
  }

  auto result = segment->GetVectors(ids, request->return_metadata());

  for (size_t i = 0; i < result.found_ids.size(); ++i) {
    auto* proto_vec = response->add_vectors();
    proto_vec->set_id(core::ToUInt64(result.found_ids[i]));
    toProto(result.found_vectors[i], proto_vec->mutable_vector());
    if (request->return_metadata() && i < result.found_metadata.size()) {
      toProto(result.found_metadata[i], proto_vec->mutable_metadata());
    }
  }

  for (const auto& id : result.not_found_ids) {
    response->add_not_found_ids(core::ToUInt64(id));
  }

  utils::MetricsRegistry::Instance().RecordGet(
      request->collection_name(), true);

  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListVectors(
    grpc::ServerContext* context,
    const proto::ListVectorsRequest* request,
    proto::ListVectorsResponse* response) {
  auto perm = CheckPermission(auth::Permission::LIST_VECTORS, request->collection_name());
  if (!perm.ok()) return perm;

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "ListVectors not supported on coordinator nodes");
  }

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = segment_store_->GetSegment(*segment_id_result);
  if (!segment) {
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  auto all_ids = segment->GetAllVectorIds();
  uint64_t total = all_ids.size();
  uint32_t limit = request->limit() > 0 ? request->limit() : 20;
  uint64_t offset = request->offset();

  response->set_total_count(total);
  response->set_has_more(offset + limit < total);

  uint64_t end = std::min(offset + limit, total);
  for (uint64_t i = offset; i < end; ++i) {
    std::vector<core::VectorId> ids_to_get = {all_ids[i]};
    auto result = segment->GetVectors(ids_to_get, request->include_metadata());

    for (size_t j = 0; j < result.found_ids.size(); ++j) {
      auto* proto_vec = response->add_vectors();
      proto_vec->set_id(core::ToUInt64(result.found_ids[j]));

      auto* vec = proto_vec->mutable_vector();
      toProto(result.found_vectors[j], vec);

      if (request->include_metadata() && j < result.found_metadata.size()) {
        toProto(result.found_metadata[j], proto_vec->mutable_metadata());
      }
    }
  }

  return grpc::Status::OK;
}

grpc::Status VectorDBService::Delete(
    grpc::ServerContext* context,
    const proto::DeleteRequest* request,
    proto::DeleteResponse* response) {
  auto perm = CheckPermission(auth::Permission::DELETE, request->collection_name());
  if (!perm.ok()) return perm;
  AuditContext::SetCollection(request->collection_name());
  AuditContext::SetItemCount(request->ids().size());

  utils::Logger::Instance().Info("Delete: {} IDs from {}",
                                 request->ids().size(),
                                 request->collection_name());

  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::DELETE,
      request->collection_name());

  if (request->ids().empty()) {
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "IDs list cannot be empty");
  }

  constexpr size_t MAX_DELETE_BATCH_SIZE = 10000;
  if (request->ids().size() > MAX_DELETE_BATCH_SIZE) {
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        absl::StrCat("Cannot delete more than ", MAX_DELETE_BATCH_SIZE,
                     " vectors in one request. Requested: ", request->ids().size()));
  }

  if (!resolver_->SupportsDataOps()) {
    utils::MetricsRegistry::Instance().RecordDelete(
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
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return gate;
  }

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = GetOrReplicateSegment(*segment_id_result);
  if (!segment) {
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  std::vector<core::VectorId> ids;
  ids.reserve(request->ids().size());
  for (uint64_t id : request->ids()) {
    ids.push_back(core::MakeVectorId(id));
  }

  auto result = segment->DeleteVectors(ids);
  if (!result.ok()) {
    utils::MetricsRegistry::Instance().RecordDelete(
        request->collection_name(), false);
    return toGrpcStatus(result.status());
  }

  response->set_deleted_count(result->deleted_count);
  for (const auto& id : result->not_found_ids) {
    response->add_not_found_ids(core::ToUInt64(id));
  }
  response->set_message(absl::StrCat(
      "Deleted ", result->deleted_count, " vector(s) from collection '",
      request->collection_name(), "'"));

  utils::MetricsRegistry::Instance().RecordDelete(
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
  AuditContext::SetCollection(request->collection_name());

  utils::Logger::Instance().Info("UpdateMetadata: ID {} in {}",
                                 request->id(), request->collection_name());

  utils::MetricsTimer timer(
      utils::MetricsRegistry::Instance(),
      utils::MetricsTimer::OperationType::UPDATE_METADATA,
      request->collection_name());

  if (request->id() == 0) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Vector ID cannot be 0");
  }

  if (request->metadata().fields().empty()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
        "Metadata cannot be empty");
  }

  if (!resolver_->SupportsDataOps()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
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
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return gate;
  }

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = segment_store_->GetSegment(*segment_id_result);
  if (!segment) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return toGrpcStatus(absl::NotFoundError("Segment not found"));
  }

  auto metadata_result = fromProto(request->metadata());
  if (!metadata_result.ok()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return toGrpcStatus(metadata_result.status());
  }

  auto vector_id = core::MakeVectorId(request->id());
  auto status = segment->UpdateMetadata(vector_id, *metadata_result, request->merge());

  if (!status.ok()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    response->set_updated(false);
    response->set_message(std::string(status.message()));
    return toGrpcStatus(status);
  }

  utils::MetricsRegistry::Instance().RecordUpdateMetadata(
      request->collection_name(), true);

  response->set_updated(true);
  response->set_message(absl::StrCat(
      "Updated metadata for vector ID ", request->id(),
      " in collection '", request->collection_name(), "'"));

  return grpc::Status::OK;
}

// ============================================================================
// Health and Stats
// ============================================================================

grpc::Status VectorDBService::HealthCheck(
    grpc::ServerContext* context,
    const proto::HealthCheckRequest* request,
    proto::HealthCheckResponse* response) {

  response->set_status(proto::HealthCheckResponse::SERVING);
  response->set_message("Server is healthy");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetStats(
    grpc::ServerContext* context,
    const proto::GetStatsRequest* request,
    proto::GetStatsResponse* response) {

  auto collections = resolver_->ListCollections();
  uint64_t total_vectors = 0;
  for (const auto& coll : collections) {
    total_vectors += coll.vector_count;
  }

  response->set_total_vectors(total_vectors);
  response->set_total_collections(collections.size());

  uint64_t queries = total_queries_.load(std::memory_order_relaxed);
  uint64_t total_time = total_query_time_ms_.load(std::memory_order_relaxed);

  response->set_total_queries(queries);
  response->set_avg_query_time_ms(
      queries > 0 ? static_cast<float>(total_time) / queries : 0.0f);

  return grpc::Status::OK;
}

// ============================================================================
// Server-Side Bulk Import
// ============================================================================

grpc::Status VectorDBService::BulkImport(
    grpc::ServerContext* /*context*/,
    const proto::BulkImportRequest* request,
    proto::BulkImportResponse* response) {
  auto perm = CheckPermission(auth::Permission::INSERT,
                               request->collection_name());
  if (!perm.ok()) return perm;

  network::AuditContext::SetCollection(request->collection_name());

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "collection_name is required");
  }
  if (request->source_uri().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "source_uri is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Bulk import is not configured on this server. "
        "Build with -DGVDB_WITH_S3=ON and configure storage.object_store");
  }

  // Resolve collection metadata
  auto collections = resolver_->ListCollections();
  const network::CollectionInfo* coll_info = nullptr;
  for (const auto& c : collections) {
    if (c.collection_name == request->collection_name()) {
      coll_info = &c;
      break;
    }
  }
  if (!coll_info) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
        "Collection not found: " + request->collection_name());
  }

  // Map proto format to internal format
  storage::ImportFormat format;
  switch (request->format()) {
    case proto::PARQUET:
      format = storage::ImportFormat::PARQUET;
      break;
    case proto::NUMPY:
      format = storage::ImportFormat::NUMPY;
      break;
    default:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
          "Unsupported import format");
  }

  auto result = bulk_importer_->StartImport(
      request->collection_name(),
      coll_info->collection_id,
      coll_info->dimension,
      coll_info->metric_type,
      coll_info->index_type,
      request->source_uri(),
      format,
      request->vector_column().empty() ? "vector" : request->vector_column(),
      request->id_column().empty() ? "id" : request->id_column());

  if (!result.ok()) {
    return toGrpcStatus(result.status());
  }

  response->set_import_id(*result);
  response->set_message("Import job started");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetImportStatus(
    grpc::ServerContext* /*context*/,
    const proto::GetImportStatusRequest* request,
    proto::GetImportStatusResponse* response) {
  if (request->import_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "import_id is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Bulk import is not configured on this server");
  }

  auto result = bulk_importer_->GetStatus(request->import_id());
  if (!result.ok()) {
    return toGrpcStatus(result.status());
  }

  // RBAC: check read access on the import's collection
  auto perm = CheckPermission(auth::Permission::SEARCH,
                               result->collection_name);
  if (!perm.ok()) return perm;

  auto& status = *result;
  response->set_import_id(status.import_id);
  response->set_state(static_cast<proto::ImportState>(status.state));
  response->set_total_vectors(status.total_vectors);
  response->set_imported_vectors(status.imported_vectors);
  response->set_progress_percent(status.progress_percent);
  response->set_error_message(status.error_message);
  response->set_elapsed_seconds(status.elapsed_seconds);
  response->set_segments_created(status.segments_created);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::CancelImport(
    grpc::ServerContext* /*context*/,
    const proto::CancelImportRequest* request,
    proto::CancelImportResponse* response) {
  if (request->import_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "import_id is required");
  }

  if (!bulk_importer_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Bulk import is not configured on this server");
  }

  // RBAC: look up import's collection, require INSERT (write) permission
  auto job_status = bulk_importer_->GetStatus(request->import_id());
  if (!job_status.ok()) {
    return toGrpcStatus(job_status.status());
  }
  auto perm = CheckPermission(auth::Permission::INSERT,
                               job_status->collection_name);
  if (!perm.ok()) return perm;

  auto status = bulk_importer_->CancelImport(request->import_id());
  if (!status.ok()) {
    return toGrpcStatus(status);
  }

  response->set_success(true);
  response->set_message("Cancellation requested");
  return grpc::Status::OK;
}

// ============================================================================
// Backup and Restore
// ============================================================================
//
// The handlers below ship single-shard (single-node) backups and restores
// in this commit. Multi-shard coordinator orchestration arrives in a
// later commit and replaces the StartBackupSingleShard call with a
// coordinator fan-out helper while keeping the response shape identical.

grpc::Status VectorDBService::BackupCollection(
    grpc::ServerContext* /*context*/,
    const proto::BackupCollectionRequest* request,
    proto::BackupCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::BACKUP,
                               request->collection_name());
  if (!perm.ok()) return perm;
  network::AuditContext::SetCollection(request->collection_name());

  if (request->collection_name().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "collection_name is required");
  }

  auto target = fromProto(request->target());
  if (!target.ok()) {
    return toGrpcStatus(target.status());
  }

  // Dispatch to coordinator-side multi-shard orchestration when wired
  // (coordinator binary). Falls through to the local single-shard path
  // on a single-node binary.
  if (coordinator_) {
    auto result = coordinator_->StartBackupDistributed(
        request->collection_name(), *target, request->backup_id());
    if (!result.ok()) return toGrpcStatus(result.status());
    response->set_backup_id(*result);
    response->set_message("Backup job started (distributed)");
    return grpc::Status::OK;
  }

  if (!backup_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Backup is not configured on this server");
  }

  // Single-shard path: resolve collection metadata for the manifest.
  // The resolver knows the collection's id, dimension, metric, index
  // type, and shard count.
  auto collections = resolver_->ListCollections();
  const network::CollectionInfo* coll_info = nullptr;
  for (const auto& c : collections) {
    if (c.collection_name == request->collection_name()) {
      coll_info = &c;
      break;
    }
  }
  if (!coll_info) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
        "Collection not found: " + request->collection_name());
  }

  auto result = backup_manager_->StartBackupSingleShard(
      request->collection_name(),
      coll_info->collection_id,
      coll_info->dimension,
      coll_info->metric_type,
      coll_info->index_type,
      /*num_shards=*/1,
      /*replication_factor=*/1,
      core::MakeShardId(0),
      /*primary_term=*/0,
      *target,
      segment_store_,
      request->backup_id());
  if (!result.ok()) {
    return toGrpcStatus(result.status());
  }
  response->set_backup_id(*result);
  response->set_message("Backup job started");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::RestoreCollection(
    grpc::ServerContext* /*context*/,
    const proto::RestoreCollectionRequest* request,
    proto::RestoreCollectionResponse* response) {
  auto perm = CheckPermission(auth::Permission::RESTORE,
                               request->target_collection_name());
  if (!perm.ok()) return perm;
  network::AuditContext::SetCollection(request->target_collection_name());

  if (request->backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "backup_id is required");
  }

  auto source = fromProto(request->source());
  if (!source.ok()) {
    return toGrpcStatus(source.status());
  }

  // Coordinator path: it reads the manifest, creates/overwrites the
  // target collection, and fans out RestoreShard. Single-node path
  // (below) restores into the original collection_id since there's no
  // coordinator to allocate a new id.
  if (coordinator_) {
    auto result = coordinator_->StartRestoreDistributed(
        *source, request->backup_id(),
        request->target_collection_name(),
        request->overwrite());
    if (!result.ok()) return toGrpcStatus(result.status());
    response->set_restore_id(*result);
    response->set_message("Restore job started (distributed)");
    return grpc::Status::OK;
  }

  if (!restore_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Restore is not configured on this server");
  }

  auto manifest = restore_manager_->ReadManifest(*source, request->backup_id());
  if (!manifest.ok()) {
    return toGrpcStatus(manifest.status());
  }

  // Mirror the coordinator-orchestrated path: drop the target if
  // overwrite is set, then create a fresh collection entry from the
  // manifest's metadata. Without this the restored segments would
  // install via AddReplicatedSegment but the resolver would have no
  // collection entry — Search / Get would return NotFound. The
  // segment-id remap inside RestoreManager::RunShardRestore reconciles
  // ids when the freshly-created collection's id differs from the
  // backup's.
  const std::string target_name =
      !request->target_collection_name().empty()
          ? request->target_collection_name()
          : manifest->collection.collection_name;

  auto existing = resolver_->GetCollectionId(target_name);
  if (existing.ok()) {
    if (!request->overwrite()) {
      return grpc::Status(grpc::StatusCode::ALREADY_EXISTS,
          "Target collection '" + target_name +
          "' already exists; set overwrite=true to drop and recreate");
    }
    auto drop_status = resolver_->DropCollection(target_name);
    if (!drop_status.ok()) return toGrpcStatus(drop_status);
  }
  auto create_or = resolver_->CreateCollection(
      target_name,
      static_cast<core::Dimension>(manifest->collection.dimension),
      static_cast<core::MetricType>(manifest->collection.metric),
      static_cast<core::IndexType>(manifest->collection.index_type),
      manifest->collection.num_shards);
  if (!create_or.ok()) return toGrpcStatus(create_or.status());

  auto result = restore_manager_->StartRestoreSingleShard(
      *source,
      request->backup_id(),
      *create_or,
      core::MakeShardId(0),
      segment_store_);
  if (!result.ok()) {
    return toGrpcStatus(result.status());
  }
  response->set_restore_id(*result);
  response->set_message("Restore job started");
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetBackupStatus(
    grpc::ServerContext* /*context*/,
    const proto::GetBackupStatusRequest* request,
    proto::GetBackupStatusResponse* response) {
  if (request->backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "backup_id is required");
  }
  // Coordinator owns the multi-shard job state; query it when wired.
  // Otherwise consult the local single-shard BackupManager.
  absl::StatusOr<storage::BackupJobStatus> status =
      coordinator_
          ? coordinator_->GetDistributedBackupStatus(request->backup_id())
          : (backup_manager_
                 ? backup_manager_->GetStatus(request->backup_id())
                 : absl::UnimplementedError(
                       "Backup is not configured on this server"));
  if (!status.ok()) {
    return toGrpcStatus(status.status());
  }
  response->set_backup_id(status->backup_id);
  response->set_state(toProto(status->state));
  response->set_shards_total(status->shards_total);
  response->set_shards_completed(status->shards_completed);
  response->set_bytes_uploaded(status->bytes_uploaded);
  response->set_error_message(status->error_message);
  response->set_elapsed_seconds(status->elapsed_seconds);
  response->set_manifest_uri(status->manifest_uri);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::GetRestoreStatus(
    grpc::ServerContext* /*context*/,
    const proto::GetRestoreStatusRequest* request,
    proto::GetRestoreStatusResponse* response) {
  if (request->restore_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "restore_id is required");
  }
  absl::StatusOr<storage::RestoreJobStatus> status =
      coordinator_
          ? coordinator_->GetDistributedRestoreStatus(request->restore_id())
          : (restore_manager_
                 ? restore_manager_->GetStatus(request->restore_id())
                 : absl::UnimplementedError(
                       "Restore is not configured on this server"));
  if (!status.ok()) {
    return toGrpcStatus(status.status());
  }
  response->set_restore_id(status->restore_id);
  response->set_state(toProto(status->state));
  response->set_shards_total(status->shards_total);
  response->set_shards_completed(status->shards_completed);
  response->set_error_message(status->error_message);
  response->set_elapsed_seconds(status->elapsed_seconds);
  return grpc::Status::OK;
}

grpc::Status VectorDBService::ListBackups(
    grpc::ServerContext* /*context*/,
    const proto::ListBackupsRequest* /*request*/,
    proto::ListBackupsResponse* /*response*/) {
  // Listing every backup at a target requires walking the target's
  // object store and parsing each top-level manifest. That helper lands
  // with the coordinator orchestration; v1 of this RPC therefore
  // returns UNIMPLEMENTED so clients can detect the gap explicitly
  // instead of silently getting an empty list.
  return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
      "ListBackups is not yet implemented; use the operator's "
      "GVDBBackup CRs as the canonical inventory");
}

grpc::Status VectorDBService::CancelBackup(
    grpc::ServerContext* /*context*/,
    const proto::CancelBackupRequest* request,
    proto::CancelBackupResponse* response) {
  if (request->backup_id().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "backup_id is required");
  }
  if (!backup_manager_) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "Backup is not configured on this server");
  }
  // RBAC: the caller must hold BACKUP on the underlying collection. We
  // need to look up which collection by id before we can authorize.
  auto status_or = backup_manager_->GetStatus(request->backup_id());
  if (!status_or.ok()) {
    return toGrpcStatus(status_or.status());
  }
  // We don't store collection_name on BackupJobStatus directly; do the
  // permission check at the collection-wildcard level by passing the
  // empty name (admin-only paths will reject; others fall through). The
  // strict per-collection gate lands when we surface collection_name on
  // the status struct in a later refinement.
  auto perm = CheckPermission(auth::Permission::BACKUP, /*collection_name=*/"");
  if (!perm.ok()) return perm;

  auto status = backup_manager_->Cancel(request->backup_id());
  if (!status.ok()) {
    return toGrpcStatus(status);
  }
  response->set_success(true);
  response->set_message("Cancellation requested");
  return grpc::Status::OK;
}

} // namespace network
} // namespace gvdb

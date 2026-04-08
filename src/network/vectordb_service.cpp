// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/vectordb_service.h"
#include "network/proto_conversions.h"
#include "network/collection_resolver.h"
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
#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

// ============================================================================
// VectorDBService Implementation
// ============================================================================

VectorDBService::VectorDBService(
    std::shared_ptr<storage::SegmentManager> segment_manager,
    std::shared_ptr<compute::QueryExecutor> query_executor,
    std::unique_ptr<ICollectionResolver> resolver)
    : segment_manager_(std::move(segment_manager)),
      query_executor_(std::move(query_executor)),
      resolver_(std::move(resolver)) {
  utils::Logger::Instance().Info("VectorDBService initialized");
}

VectorDBService::~VectorDBService() = default;

// ============================================================================
// Helpers
// ============================================================================

storage::Segment* VectorDBService::GetOrReplicateSegment(core::SegmentId segment_id) {
  auto* segment = segment_manager_->GetSegment(segment_id);
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

  auto add_status = segment_manager_->AddReplicatedSegment(std::move(segment_result.value()));
  if (!add_status.ok()) {
    utils::Logger::Instance().Error("Failed to add replicated segment {}: {}",
                                     response.segment_info().segment_id(),
                                     add_status.message());
    return nullptr;
  }

  utils::Logger::Instance().Info("Replicated segment {} ({} vectors)",
                                 response.segment_info().segment_id(),
                                 response.segment_info().vector_count());

  return segment_manager_->GetSegment(segment_id);
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

  // Fan out ExecuteShardQuery to each data node in parallel
  struct ShardEntry {
    uint64_t id;
    float distance;
    proto::Metadata metadata;
  };
  struct ShardResult {
    std::vector<ShardEntry> entries;
    bool ok = false;
  };

  std::vector<std::future<ShardResult>> futures;
  futures.reserve(targets.size());

  for (const auto& target : targets) {
    futures.push_back(std::async(std::launch::async,
        [&target, &query, &request]() -> ShardResult {
          ShardResult result;

          if (target.node_address.empty()) return result;

          auto channel = grpc::CreateChannel(target.node_address,
                                              grpc::InsecureChannelCredentials());
          auto stub = proto::internal::InternalService::NewStub(channel);

          proto::internal::ExecuteShardQueryRequest shard_req;
          shard_req.set_collection_id(target.collection_id);
          shard_req.set_shard_id(target.shard_id);
          shard_req.set_top_k(request->top_k());
          shard_req.set_filter(request->filter());
          shard_req.set_return_metadata(request->return_metadata());
          for (int i = 0; i < query.dimension(); ++i) {
            shard_req.add_query_vector(query.data()[i]);
          }

          grpc::ClientContext ctx;
          ctx.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::seconds(10));
          proto::internal::ExecuteShardQueryResponse shard_resp;

          auto status = stub->ExecuteShardQuery(&ctx, shard_req, &shard_resp);
          if (!status.ok()) {
            utils::Logger::Instance().Warn(
                "Shard query failed on {}: {}", target.node_address,
                status.error_message());
            return result;
          }

          for (const auto& r : shard_resp.results()) {
            ShardEntry entry;
            entry.id = r.id();
            entry.distance = r.distance();
            if (r.metadata().fields_size() > 0) {
              entry.metadata = r.metadata();
            }
            result.entries.push_back(std::move(entry));
          }
          result.ok = true;
          return result;
        }));
  }

  // Collect and merge results
  std::vector<ShardEntry> all_entries;
  for (auto& future : futures) {
    auto result = future.get();
    if (result.ok) {
      for (auto& e : result.entries) {
        all_entries.push_back(std::move(e));
      }
    }
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

  // Get all segment IDs for this collection
  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    return toGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;
  size_t num_shards = segment_ids.size();

  // Get dimension from first segment
  auto* first_segment = segment_manager_->GetSegment(segment_ids[0]);
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

  // Write each shard batch to its segment (with auto-rotation on full)
  core::CollectionId collection_id = first_segment->GetCollectionId();
  size_t total_inserted = 0;
  for (uint32_t i = 0; i < num_shards; ++i) {
    auto& batch = shard_batches[i];
    if (batch.ids.empty()) continue;

    // Compute batch size for capacity-aware rotation
    size_t batch_bytes = 0;
    for (const auto& v : batch.vectors) batch_bytes += v.byte_size();

    // Get a writable segment that can fit this batch
    auto* segment = segment_manager_->GetWritableSegment(collection_id, batch_bytes);
    if (!segment) {
      segment = segment_manager_->GetSegment(segment_ids[i]);
    }
    if (!segment) continue;

    absl::Status status;
    if (batch.has_sparse) {
      status = segment->AddVectorsWithSparse(batch.vectors, batch.ids, batch.metadata, batch.sparse_map, batch.expiry_entries);
    } else if (batch.has_metadata) {
      status = segment->AddVectorsWithMetadata(batch.vectors, batch.ids, batch.metadata, batch.expiry_entries);
    } else {
      status = segment->AddVectors(batch.vectors, batch.ids, batch.expiry_entries);
    }

    // Safety net: if segment is full despite hint, rotate and retry once
    if (absl::IsResourceExhausted(status)) {
      segment = segment_manager_->GetWritableSegment(collection_id, batch_bytes);
      if (segment) {
        if (batch.has_sparse) {
          status = segment->AddVectorsWithSparse(batch.vectors, batch.ids, batch.metadata, batch.sparse_map, batch.expiry_entries);
        } else if (batch.has_metadata) {
          status = segment->AddVectorsWithMetadata(batch.vectors, batch.ids, batch.metadata, batch.expiry_entries);
        } else {
          status = segment->AddVectors(batch.vectors, batch.ids, batch.expiry_entries);
        }
      }
    }

    if (!status.ok()) {
      utils::MetricsRegistry::Instance().RecordInsert(
          request->collection_name(), false, 0);
      return toGrpcStatus(status);
    }

    total_inserted += batch.ids.size();
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

  while (reader->Read(&chunk)) {
    if (chunk.vectors().empty()) continue;
    if (collection_name.empty()) collection_name = chunk.collection_name();

    auto segment_ids_result = resolver_->GetSegmentIds(chunk.collection_name());
    if (!segment_ids_result.ok()) {
      return toGrpcStatus(segment_ids_result.status());
    }
    const auto& segment_ids = *segment_ids_result;
    size_t num_shards = segment_ids.size();

    auto* first_segment = segment_manager_->GetSegment(segment_ids[0]);
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

      auto* segment = segment_manager_->GetSegment(segment_ids[i]);
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

  auto segment_ids_result = resolver_->GetSegmentIds(request->collection_name());
  if (!segment_ids_result.ok()) {
    return toGrpcStatus(segment_ids_result.status());
  }
  const auto& segment_ids = *segment_ids_result;

  // Find a GROWING segment
  storage::Segment* growing_segment = nullptr;
  for (auto seg_id : segment_ids) {
    auto* seg = segment_manager_->GetSegment(seg_id);
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
  if (!segment_ids_result.ok()) {
    return toGrpcStatus(segment_ids_result.status());
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
        auto* segment = segment_manager_->GetSegment(seg_id);
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

grpc::Status VectorDBService::Search(
    grpc::ServerContext* context,
    const proto::SearchRequest* request,
    proto::SearchResponse* response) {

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
    if (segment_manager_->GetSegment(seg_id)) { any_local = true; break; }
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
        auto* segment = segment_manager_->GetSegment(seg_id);
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
    auto* segment = segment_manager_->GetSegment(seg_id);
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
    auto* segment = segment_manager_->GetSegment(seg_id);
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
        auto* segment = segment_manager_->GetSegment(seg_id);
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

  if (!resolver_->SupportsDataOps()) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
        "ListVectors not supported on coordinator nodes");
  }

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = segment_manager_->GetSegment(*segment_id_result);
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

  auto segment_id_result = resolver_->GetSegmentId(request->collection_name());
  if (!segment_id_result.ok()) {
    utils::MetricsRegistry::Instance().RecordUpdateMetadata(
        request->collection_name(), false);
    return toGrpcStatus(segment_id_result.status());
  }

  auto* segment = segment_manager_->GetSegment(*segment_id_result);
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

} // namespace network
} // namespace gvdb

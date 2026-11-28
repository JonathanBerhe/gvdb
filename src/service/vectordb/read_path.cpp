// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// The local read paths. The fan-out to the data nodes is in
// distributed_search.cpp.

#include "service/vectordb/read_path.h"

#include <algorithm>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/metrics.h"
#include "common/timer.h"
#include "core/filter.h"
#include "index/bm25_index.h"
#include "index/sparse_index.h"
#include "rpc/proto_conversions.h"

namespace gvdb::service {

namespace {

// A vector resolved by the fan-out lookup below.
struct FoundVector {
  core::Vector vector;
  core::Metadata metadata;
  bool has_metadata = false;
};

// Look up each requested id across the given segments, stopping at the first
// segment that holds it. Returns a map keyed by the raw uint64 id so callers
// can emit results in their own requested order. include_metadata mirrors the
// per-segment GetVectors contract: when set, a found vector always carries a
// metadata entry (possibly empty), so has_metadata tracks presence of that
// slot rather than of any fields.
std::unordered_map<uint64_t, FoundVector> FetchAcrossSegments(
    const std::vector<storage::Segment*>& segments, const std::vector<core::VectorId>& ids,
    bool include_metadata) {
  std::unordered_map<uint64_t, FoundVector> found;
  std::vector<core::VectorId> remaining = ids;

  for (auto* segment : segments) {
    if (remaining.empty()) break;
    auto result = segment->GetVectors(remaining, include_metadata);
    for (size_t i = 0; i < result.found_ids.size(); ++i) {
      // Aggregate-init: core::Vector has no default constructor.
      FoundVector fv{std::move(result.found_vectors[i]), core::Metadata{}, false};
      if (include_metadata && i < result.found_metadata.size()) {
        fv.metadata = std::move(result.found_metadata[i]);
        fv.has_metadata = true;
      }
      found.emplace(core::ToUInt64(result.found_ids[i]), std::move(fv));
    }

    std::vector<core::VectorId> next;
    next.reserve(remaining.size());
    for (const auto& id : remaining) {
      if (!found.contains(core::ToUInt64(id))) {
        next.push_back(id);
      }
    }
    remaining = std::move(next);
  }

  return found;
}

// Parses the filter of a request. An empty filter gives no node.
absl::StatusOr<std::unique_ptr<core::FilterNode>> ParseRequestFilter(const std::string& filter) {
  if (filter.empty()) return std::unique_ptr<core::FilterNode>();
  return core::FilterParser::parse(filter);
}

}  // namespace

ReadPath::ReadPath(std::shared_ptr<storage::ISegmentStore> segment_store,
                   std::shared_ptr<compute::QueryExecutor> query_executor,
                   ICollectionResolver& resolver, SegmentLocator& locator)
    : segment_store_(std::move(segment_store)),
      query_executor_(std::move(query_executor)),
      resolver_(resolver),
      locator_(locator) {}

grpc::Status ReadPath::Search(const proto::SearchRequest& request,
                              proto::SearchResponse* response) {
  common::Timer search_timer;

  // Get collection ID
  auto collection_id_result = resolver_.GetCollectionId(request.collection_name());
  if (!collection_id_result.ok()) {
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
    return rpc::ToGrpcStatus(collection_id_result.status());
  }

  // Convert query vector
  auto query_result = rpc::FromProto(request.query_vector());
  if (!query_result.ok()) {
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
    return rpc::ToGrpcStatus(query_result.status());
  }

  // The data nodes parse the filter of a fan-out, so a bad filter is caught
  // here first and is an INVALID_ARGUMENT error, not an empty result. The
  // local path parses it in QueryExecutor.
  auto fan_out = [&]() -> grpc::Status {
    if (auto filter = ParseRequestFilter(request.filter()); !filter.ok()) {
      common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
      return rpc::ToGrpcStatus(filter.status());
    }
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), true);
    return SearchDistributed(&request, response, *query_result);
  };

  // Get all segments for this collection
  auto segment_ids_result = resolver_.GetSegmentIds(request.collection_name());
  if (!segment_ids_result.ok()) {
    // Try distributed search
    auto targets_result = resolver_.GetShardTargets(request.collection_name());
    if (targets_result.ok() && !targets_result->empty()) {
      return fan_out();
    }
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
    return rpc::ToGrpcStatus(segment_ids_result.status());
  }

  const auto& segment_ids = *segment_ids_result;

  // Check if any segment exists locally
  bool any_local = false;
  for (const auto& seg_id : segment_ids) {
    if (segment_store_->GetSegment(seg_id)) {
      any_local = true;
      break;
    }
  }

  if (!any_local) {
    // No local segments: try distributed search
    auto targets_result = resolver_.GetShardTargets(request.collection_name());
    if (targets_result.ok() && !targets_result->empty()) {
      return fan_out();
    }
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
    return rpc::ToGrpcStatus(absl::NotFoundError("No segments found"));
  }

  // Delegate search to QueryExecutor (handles caching, filtering, multi-segment merge)
  core::CollectionId collection_id = *collection_id_result;
  auto search_result =
      query_executor_->Search(collection_id, *query_result, request.top_k(), request.filter());

  if (!search_result.ok()) {
    common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), false);
    return rpc::ToGrpcStatus(search_result.status());
  }

  common::MetricsRegistry::Instance().RecordSearch(request.collection_name(), true);

  // Build response with optional metadata enrichment
  for (const auto& entry : search_result->entries) {
    auto* proto_entry = response->add_results();
    rpc::ToProto(entry, proto_entry);

    if (request.return_metadata()) {
      if (auto meta = locator_.FindMetadata(entry.id, segment_ids)) {
        rpc::ToProto(*meta, proto_entry->mutable_metadata());
      }
    }
  }

  response->set_query_time_ms(search_timer.ElapsedMillis());

  total_queries_.fetch_add(1, std::memory_order_relaxed);
  total_query_time_ms_.fetch_add(static_cast<uint64_t>(search_timer.ElapsedMillis()),
                                 std::memory_order_relaxed);

  return grpc::Status::OK;
}

grpc::Status ReadPath::RangeSearch(const proto::RangeSearchRequest& request,
                                   proto::RangeSearchResponse* response) {
  common::Timer timer;

  // Parse the filter once for every segment. A bad filter is an
  // INVALID_ARGUMENT error, not an empty result.
  auto filter = ParseRequestFilter(request.filter());
  if (!filter.ok()) {
    return rpc::ToGrpcStatus(filter.status());
  }

  auto segment_ids_result = resolver_.GetSegmentIds(request.collection_name());

  // Check if we have local segments; if not, fan out to data nodes
  bool any_local = false;
  if (segment_ids_result.ok()) {
    for (const auto& seg_id : *segment_ids_result) {
      if (segment_store_->GetSegment(seg_id)) {
        any_local = true;
        break;
      }
    }
  }

  if (!any_local) {
    auto targets = resolver_.GetShardTargets(request.collection_name());
    if (targets.ok() && !targets->empty()) {
      return RangeSearchDistributed(&request, response);
    }
    if (!segment_ids_result.ok()) {
      return rpc::ToGrpcStatus(segment_ids_result.status());
    }
  }

  const auto& segment_ids = *segment_ids_result;

  auto query_result = rpc::FromProto(request.query_vector());
  if (!query_result.ok()) {
    return rpc::ToGrpcStatus(query_result.status());
  }

  int max_results = request.max_results() > 0 ? request.max_results() : 1000;

  // Search each segment and merge results
  std::vector<core::SearchResultEntry> all_results;
  for (auto seg_id : segment_ids) {
    auto* segment = locator_.GetOrReplicate(seg_id);
    if (!segment) continue;

    absl::StatusOr<core::SearchResult> seg_result;
    if (!*filter) {
      seg_result = segment->SearchRange(*query_result, request.radius(), max_results);
    } else {
      seg_result =
          segment->SearchRangeWithFilter(*query_result, request.radius(), **filter, max_results);
    }

    if (seg_result.ok()) {
      all_results.insert(all_results.end(), seg_result->entries.begin(), seg_result->entries.end());
    }
  }

  // Sort by distance and limit
  std::ranges::sort(all_results,
                    [](const auto& a, const auto& b) { return a.distance < b.distance; });
  if (static_cast<int>(all_results.size()) > max_results) {
    all_results.resize(max_results);
  }

  // Build response
  for (const auto& entry : all_results) {
    auto* proto_entry = response->add_results();
    rpc::ToProto(entry, proto_entry);

    if (request.return_metadata()) {
      if (auto meta = locator_.FindMetadata(entry.id, segment_ids)) {
        rpc::ToProto(*meta, proto_entry->mutable_metadata());
      }
    }
  }

  response->set_query_time_ms(static_cast<float>(timer.ElapsedMillis()));
  total_queries_++;

  return grpc::Status::OK;
}

grpc::Status ReadPath::HybridSearch(const proto::HybridSearchRequest& request,
                                    proto::HybridSearchResponse* response) {
  common::Timer search_timer;

  bool has_vector = request.has_query_vector() && request.query_vector().dimension() > 0;
  bool has_text = !request.text_query().empty();
  bool has_sparse = request.has_sparse_query() && request.sparse_query().indices_size() > 0;

  // A bad filter is an INVALID_ARGUMENT error. A valid one limits the
  // candidates of every segment.
  auto filter = ParseRequestFilter(request.filter());
  if (!filter.ok()) {
    return rpc::ToGrpcStatus(filter.status());
  }

  // Resolve collection
  auto collection_id_result = resolver_.GetCollectionId(request.collection_name());
  if (!collection_id_result.ok()) {
    return rpc::ToGrpcStatus(collection_id_result.status());
  }

  // Convert query vector (optional, may be text-only)
  std::optional<core::Vector> query_vector;
  if (has_vector) {
    auto qr = rpc::FromProto(request.query_vector());
    if (!qr.ok()) return rpc::ToGrpcStatus(qr.status());
    query_vector = std::move(*qr);
  }

  // Convert sparse query (optional)
  std::optional<core::SparseVector> sparse_query;
  if (has_sparse) {
    auto sq = rpc::FromProto(request.sparse_query());
    if (!sq.ok()) return rpc::ToGrpcStatus(sq.status());
    sparse_query = std::move(*sq);
  }
  float sparse_weight = request.sparse_weight() > 0 ? request.sparse_weight() : 0.0f;

  // Get segments
  auto segment_ids_result = resolver_.GetSegmentIds(request.collection_name());
  if (!segment_ids_result.ok()) {
    return rpc::ToGrpcStatus(segment_ids_result.status());
  }

  // Determine weights
  float vector_weight = request.vector_weight() > 0 ? request.vector_weight() : 0.5f;
  float text_weight = request.text_weight() > 0 ? request.text_weight() : 0.5f;
  std::string text_field = request.text_field().empty() ? "text" : request.text_field();

  // Build text index on-the-fly for segments that don't have one
  for (const auto& seg_id : *segment_ids_result) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;

    // Build text index from metadata if not already built
    auto bm25 = std::make_unique<index::BM25Index>();
    segment->BuildTextIndex(std::move(bm25), text_field).IgnoreError();

    // Build sparse index from stored sparse vectors if query needs it
    if (has_sparse) {
      auto sparse_idx = std::make_unique<index::SparseIndex>();
      segment->BuildSparseIndex(std::move(sparse_idx)).IgnoreError();
    }
  }

  // Search each segment
  std::vector<core::SearchResultEntry> all_entries;
  for (const auto& seg_id : *segment_ids_result) {
    auto* segment = segment_store_->GetSegment(seg_id);
    if (!segment) continue;

    absl::StatusOr<core::SearchResult> result;

    const core::SparseVector* sq_ptr = sparse_query ? &*sparse_query : nullptr;

    if (has_vector || has_text || has_sparse) {
      // Hybrid: any combination of vector + text + sparse with RRF fusion
      core::Vector qv = has_vector ? *query_vector : core::ZeroVector(segment->GetDimension());
      float vw = has_vector ? vector_weight : 0.0f;
      float tw = has_text ? text_weight : 0.0f;
      float sw = has_sparse ? sparse_weight : 0.0f;

      result = segment->SearchHybrid(qv, request.text_query(), request.top_k(), vw, tw, text_field,
                                     sq_ptr, sw, filter->get());
    }

    if (result.ok()) {
      for (auto& entry : result->entries) {
        all_entries.push_back(entry);
      }
    }
  }

  // Sort by RRF score (higher = better) and take top_k
  std::ranges::sort(all_entries,
                    [](const auto& a, const auto& b) { return a.distance > b.distance; });

  int top_k = std::min(static_cast<int>(all_entries.size()), static_cast<int>(request.top_k()));

  for (int i = 0; i < top_k; ++i) {
    auto* proto_entry = response->add_results();
    rpc::ToProto(all_entries[i], proto_entry);

    if (request.return_metadata()) {
      if (auto meta = locator_.FindMetadata(all_entries[i].id, *segment_ids_result)) {
        rpc::ToProto(*meta, proto_entry->mutable_metadata());
      }
    }
  }

  response->set_query_time_ms(search_timer.ElapsedMillis());
  return grpc::Status::OK;
}

grpc::Status ReadPath::Get(const proto::GetRequest& request, proto::GetResponse* response) {
  // Consult every segment of the collection, not just the resolver's active
  // one: once a segment rotates, ids move to rotated segments and a
  // single-segment lookup would report them as not found.
  auto segments_result = locator_.CollectionSegments(request.collection_name());
  if (!segments_result.ok()) {
    common::MetricsRegistry::Instance().RecordGet(request.collection_name(), false);
    return rpc::ToGrpcStatus(segments_result.status());
  }

  const auto ids =
      request.ids() | std::views::transform(core::MakeVectorId) | std::ranges::to<std::vector>();

  auto found = FetchAcrossSegments(*segments_result, ids, request.return_metadata());

  // Emit in the requested id order; ids resolved by no segment are reported
  // as not found.
  for (const auto& id : ids) {
    auto it = found.find(core::ToUInt64(id));
    if (it == found.end()) {
      response->add_not_found_ids(core::ToUInt64(id));
      continue;
    }
    auto* proto_vec = response->add_vectors();
    proto_vec->set_id(core::ToUInt64(id));
    rpc::ToProto(it->second.vector, proto_vec->mutable_vector());
    if (request.return_metadata() && it->second.has_metadata) {
      rpc::ToProto(it->second.metadata, proto_vec->mutable_metadata());
    }
  }

  common::MetricsRegistry::Instance().RecordGet(request.collection_name(), true);

  return grpc::Status::OK;
}

grpc::Status ReadPath::ListVectors(const proto::ListVectorsRequest& request,
                                   proto::ListVectorsResponse* response) {
  // List across every segment of the collection so rotated segments are
  // included; a single-segment listing would miss everything past the first.
  auto segments_result = locator_.CollectionSegments(request.collection_name());
  if (!segments_result.ok()) {
    return rpc::ToGrpcStatus(segments_result.status());
  }
  const auto& segments = *segments_result;

  std::vector<core::VectorId> all_ids;
  for (auto* segment : segments) {
    auto seg_ids = segment->GetAllVectorIds();
    all_ids.insert(all_ids.end(), seg_ids.begin(), seg_ids.end());
  }

  uint64_t total = all_ids.size();
  uint32_t limit = request.limit() > 0 ? request.limit() : 20;
  uint64_t offset = request.offset();

  response->set_total_count(total);
  response->set_has_more(offset + limit < total);

  uint64_t end = std::min(offset + limit, total);
  std::vector<core::VectorId> page_ids;
  if (offset < end) {
    page_ids.assign(all_ids.begin() + offset, all_ids.begin() + end);
  }

  auto found = FetchAcrossSegments(segments, page_ids, request.include_metadata());

  for (const auto& id : page_ids) {
    auto it = found.find(core::ToUInt64(id));
    if (it == found.end()) continue;
    auto* proto_vec = response->add_vectors();
    proto_vec->set_id(core::ToUInt64(id));
    rpc::ToProto(it->second.vector, proto_vec->mutable_vector());
    if (request.include_metadata() && it->second.has_metadata) {
      rpc::ToProto(it->second.metadata, proto_vec->mutable_metadata());
    }
  }

  return grpc::Status::OK;
}

}  // namespace gvdb::service

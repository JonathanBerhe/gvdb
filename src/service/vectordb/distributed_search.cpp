// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Search and range search fanned out to the shards through the coordinator.

#include <grpcpp/grpcpp.h>
#include <algorithm>
#include <chrono>
#include <future>
#include <set>
#include "absl/strings/str_cat.h"
#include "common/logger.h"
#include "common/timer.h"
#include "internal.grpc.pb.h"
#include "rpc/proto_conversions.h"
#include "rpc/replica_fallback.h"
#include "service/vectordb/collection_resolver.h"
#include "service/vectordb/vectordb_service.h"

namespace gvdb {
namespace service {

grpc::Status VectorDBService::SearchDistributed(const proto::SearchRequest* request,
                                                proto::SearchResponse* response,
                                                const core::Vector& query) {
  common::Timer timer;

  // Get shard targets from coordinator
  auto targets_result = resolver_->GetShardTargets(request->collection_name());
  if (!targets_result.ok()) {
    return rpc::ToGrpcStatus(targets_result.status());
  }

  const auto& targets = *targets_result;
  if (targets.empty()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        "No shards found for collection: " + request->collection_name());
  }

  common::Logger::Instance().Info("Distributed search: {} shards for '{}'", targets.size(),
                                  request->collection_name());

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
    futures.push_back(std::async(std::launch::async, [&target, &query, &request]() -> ShardResult {
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
      // consumes. Same data, different message: the resolver layer
      // intentionally keeps internal.pb.h out of its public header.
      google::protobuf::RepeatedPtrField<proto::internal::RouteQueryNodeOption> proto_options;
      for (const auto& o : target.options) {
        auto* p = proto_options.Add();
        p->set_node_id(o.node_id);
        p->set_node_address(o.node_address);
        p->set_is_primary(o.is_primary);
      }

      auto rpc_result = rpc::CallWithReplicaFallback(
          proto_options, std::chrono::milliseconds(10000), "search",
          [&](grpc::ClientContext* ctx, const std::string& addr) -> grpc::Status {
            // Defensive clear: today's helper returns on first OK,
            // so this lambda runs at most once with a successful
            // status. If a future maintainer changes the helper's
            // strategy (e.g. best-of-N) this prevents accumulating
            // duplicate entries from multiple successful retries.
            result.entries.clear();

            auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
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

            proto::internal::ExecuteShardQueryResponse shard_resp;
            auto status = stub->ExecuteShardQuery(ctx, shard_req, &shard_resp);
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
      result.last_error = std::string(rpc_result.final_status.error_message());
      if (!result.ok) {
        common::Logger::Instance().Warn("Shard {} search exhausted candidates: {}", target.shard_id,
                                        result.last_error);
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
                     "] exhausted all candidates (last error: ", last_error_summary, ")"));
  }

  // Sort by distance ascending (L2/cosine) and take top_k
  std::sort(all_entries.begin(), all_entries.end(),
            [](const auto& a, const auto& b) { return a.distance < b.distance; });

  int top_k = std::min(static_cast<int>(all_entries.size()), static_cast<int>(request->top_k()));

  for (int i = 0; i < top_k; ++i) {
    auto* proto_entry = response->add_results();
    proto_entry->set_id(all_entries[i].id);
    proto_entry->set_distance(all_entries[i].distance);

    if (request->return_metadata() && all_entries[i].metadata.fields_size() > 0) {
      *proto_entry->mutable_metadata() = all_entries[i].metadata;
    }
  }

  response->set_query_time_ms(timer.ElapsedMillis());

  total_queries_.fetch_add(1, std::memory_order_relaxed);
  total_query_time_ms_.fetch_add(static_cast<uint64_t>(timer.ElapsedMillis()),
                                 std::memory_order_relaxed);

  return grpc::Status::OK;
}

grpc::Status VectorDBService::RangeSearchDistributed(const proto::RangeSearchRequest* request,
                                                     proto::RangeSearchResponse* response) {
  common::Timer timer;

  auto targets_result = resolver_->GetShardTargets(request->collection_name());
  if (!targets_result.ok()) {
    return rpc::ToGrpcStatus(targets_result.status());
  }

  const auto& targets = *targets_result;
  if (targets.empty()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        "No shards found for collection: " + request->collection_name());
  }

  // Deduplicate node addresses: multiple shards may live on the same
  // node, in which case one RangeSearch RPC covers all of them. We do
  // NOT use replica fallback at the per-node level here: RangeSearch is
  // collection-scoped (no shard selector), so a "replica" of one shard
  // may not host the other shards the original node owned. Falling over
  // to such a replica would return a successful-but-truncated response:
  // re-introducing the silent partial-truncation hazard this PR
  // explicitly fixes elsewhere. Per-shard fallback for RangeSearch
  // requires a shard-scoped RangeSearch RPC and is intentionally
  // deferred; until then, a single-node failure surfaces UNAVAILABLE
  // listing the failed node: visible, not silent.
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
    futures.push_back(std::async(std::launch::async, [addr, &request]() -> NodeResult {
      NodeResult result;
      result.addr = addr;

      auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      auto stub = proto::VectorDBService::NewStub(channel);

      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

      auto status = stub->RangeSearch(&ctx, *request, &result.resp);
      result.ok = status.ok();
      result.last_status_code = status.error_code();
      result.last_error = std::string(status.error_message());
      if (!result.ok) {
        common::Logger::Instance().Warn("Distributed RangeSearch failed on {}: {}", result.addr,
                                        result.last_error);
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
                     "] exhausted all candidates (last error: ", last_error_summary, ")"));
  }

  // Sort by distance and limit
  std::sort(all_entries.begin(), all_entries.end(),
            [](const auto& a, const auto& b) { return a.distance() < b.distance(); });
  if (static_cast<int>(all_entries.size()) > max_results) {
    all_entries.resize(max_results);
  }

  for (auto& entry : all_entries) {
    *response->add_results() = std::move(entry);
  }

  response->set_query_time_ms(static_cast<float>(timer.ElapsedMillis()));
  total_queries_++;

  return grpc::Status::OK;
}

}  // namespace service
}  // namespace gvdb

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>

#include <google/protobuf/repeated_ptr_field.h>
#include <grpcpp/grpcpp.h>

#include "internal.grpc.pb.h"
#include "internal.pb.h"
#include "utils/logger.h"
#include "utils/metrics.h"

namespace gvdb {
namespace network {

// Per-attempt deadline lower bound. A tight overall deadline divided by
// many candidates can produce sub-millisecond budgets; clamp so every
// attempt has enough headroom for a real round-trip on a healthy node.
inline constexpr std::chrono::milliseconds kReplicaFallbackMinPerAttempt{20};

// Per-attempt deadline upper bound. A generous overall deadline divided
// by few candidates can produce many-second budgets; cap so a single
// dead node can't stall failover by more than a couple of seconds.
inline constexpr std::chrono::milliseconds kReplicaFallbackMaxPerAttempt{
    2000};

// Outcome of a CallWithReplicaFallback invocation. final_status is OK iff
// at least one option succeeded; on a non-OK return, attempts records how
// many options were tried before bailing (1 if a non-transient status
// short-circuited, options.size() if every transient retry was exhausted).
struct ReplicaFallbackResult {
  grpc::Status final_status;
  uint32_t attempts = 0;
  uint32_t target_node_id_used = 0;
  bool used_replica_fallback = false;
};

// Returns true iff the gRPC status is the kind of transient routing
// failure another candidate node might satisfy (target unreachable,
// per-attempt deadline elapsed). Semantic failures (NOT_FOUND, INTERNAL,
// INVALID_ARGUMENT, ...) recur on every node and short-circuit the loop.
bool IsTransientReplicaError(grpc::StatusCode code);

// Iterate a per-shard candidate list and attempt the supplied gRPC call
// against each option in order. Returns on the first OK status, on the
// first non-transient error, or after exhausting every option.
//
// CallFn signature:
//
//     grpc::Status CallFn(grpc::ClientContext* context,
//                         const std::string& target_address);
//
// The helper sets a per-attempt deadline derived from total_deadline and
// the candidate count, bounded to [20ms, 2s] so a tight overall deadline
// still gives every attempt enough headroom while a generous one fails
// over fast.
//
// Observability: every call observes gvdb_read_attempts; the first
// successful fallback (i > 0) bumps gvdb_read_replica_fallback_total with
// reason="primary_unreachable" (when options[0] was the primary) or
// reason="replica_unreachable" (when options[0] was already a replica
// because the primary was draining at routing time); a fully exhausted
// list bumps gvdb_read_exhausted_replicas_total. operation_label
// identifies the read path (e.g. "get", "hybrid_search").
template <typename CallFn>
ReplicaFallbackResult CallWithReplicaFallback(
    const google::protobuf::RepeatedPtrField<
        proto::internal::RouteQueryNodeOption>& options,
    std::chrono::milliseconds total_deadline,
    const std::string& operation_label,
    CallFn&& call_fn) {
  ReplicaFallbackResult result;

  if (options.empty()) {
    utils::Logger::Instance().Warn(
        "replica_fallback[{}]: empty candidate list; surfacing FAILED_PRECONDITION",
        operation_label);
    result.final_status = grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                                        "no routable nodes for shard");
    return result;
  }

  std::chrono::milliseconds per_attempt = total_deadline / options.size();
  if (per_attempt < kReplicaFallbackMinPerAttempt) {
    per_attempt = kReplicaFallbackMinPerAttempt;
  }
  if (per_attempt > kReplicaFallbackMaxPerAttempt) {
    per_attempt = kReplicaFallbackMaxPerAttempt;
  }

  grpc::Status last_status =
      grpc::Status(grpc::StatusCode::UNAVAILABLE, "no attempts made");

  auto& metrics = utils::MetricsRegistry::Instance();

  for (int i = 0; i < options.size(); ++i) {
    const auto& opt = options[i];

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + per_attempt);

    utils::Logger::Instance().Debug(
        "replica_fallback[{}]: attempt {}/{} target=node_{} addr={} primary={}",
        operation_label, result.attempts + 1, options.size(),
        opt.node_id(), opt.node_address(), opt.is_primary());

    grpc::Status status = std::forward<CallFn>(call_fn)(&context, opt.node_address());
    ++result.attempts;
    last_status = status;
    result.target_node_id_used = opt.node_id();

    if (status.ok()) {
      result.final_status = status;
      result.used_replica_fallback = (i > 0);
      metrics.RecordReadAttempts(operation_label, result.attempts);
      if (result.used_replica_fallback) {
        const std::string reason = options[0].is_primary()
                                       ? "primary_unreachable"
                                       : "replica_unreachable";
        metrics.IncReadReplicaFallback(operation_label, reason);
        utils::Logger::Instance().Info(
            "replica_fallback[{}]: succeeded on candidate {}/{} "
            "(node_{}, reason={}, attempts={})",
            operation_label, i + 1, options.size(), opt.node_id(),
            reason, result.attempts);
      }
      return result;
    }

    // Non-transient: another replica won't fix the problem, surface now.
    if (!IsTransientReplicaError(status.error_code())) {
      utils::Logger::Instance().Debug(
          "replica_fallback[{}]: non-transient status {} on node_{}; "
          "short-circuiting (every replica would return the same error)",
          operation_label, static_cast<int>(status.error_code()), opt.node_id());
      result.final_status = status;
      metrics.RecordReadAttempts(operation_label, result.attempts);
      return result;
    }

    utils::Logger::Instance().Debug(
        "replica_fallback[{}]: transient status {} on node_{}; trying next candidate",
        operation_label, static_cast<int>(status.error_code()), opt.node_id());
  }

  // Every candidate was tried and every attempt was a transient failure.
  utils::Logger::Instance().Warn(
      "replica_fallback[{}]: exhausted all {} candidate(s); "
      "last status code={} message=\"{}\"",
      operation_label, options.size(),
      static_cast<int>(last_status.error_code()),
      last_status.error_message());
  result.final_status = last_status;
  metrics.RecordReadAttempts(operation_label, result.attempts);
  metrics.IncReadExhaustedReplicas(operation_label);
  return result;
}

// Convenience wrapper that pairs a RouteQuery RPC with
// CallWithReplicaFallback. Reads are collection-scoped from the proxy's
// perspective; the coordinator's RouteQuery answers with per-shard
// candidates and this helper iterates the FIRST shard's options. (Today's
// proxy read paths assume one logical shard owns the request — this
// preserves that semantic; multi-shard fan-out is the query-node's job.)
//
// dial_fn signature:   (const std::string& target_address) -> StubT*
// call_fn signature:   (grpc::ClientContext*, StubT*) -> grpc::Status
//
// Returns OK iff at least one candidate succeeded; otherwise the final
// (transient or non-transient) status from the helper.
template <typename DialFn, typename CallFn>
grpc::Status RouteAndCallWithFallback(
    proto::internal::InternalService::Stub* internal_client,
    const std::string& collection_name,
    const std::string& operation_label,
    std::chrono::milliseconds total_deadline,
    DialFn&& dial_fn,
    CallFn&& call_fn) {
  if (!internal_client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                         "no coordinator client to call RouteQuery");
  }

  proto::internal::RouteQueryRequest route_req;
  route_req.set_collection_name(collection_name);
  route_req.set_top_k(0);
  route_req.set_prefer_routable_replica(true);

  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext route_ctx;
  auto route_status =
      internal_client->RouteQuery(&route_ctx, route_req, &route_resp);
  if (!route_status.ok()) {
    utils::Logger::Instance().Warn(
        "replica_fallback[{}]: RouteQuery failed: {}", operation_label,
        route_status.error_message());
    return route_status;
  }
  // Prefer the new per-shard options list when the coordinator emits it;
  // synthesize a single-option list from the legacy parallel arrays for
  // back-compat with older coordinator binaries (and unit-test mocks
  // that haven't been updated to populate per_shard_options yet).
  google::protobuf::RepeatedPtrField<proto::internal::RouteQueryNodeOption>
      synthesized_options;
  const google::protobuf::RepeatedPtrField<proto::internal::RouteQueryNodeOption>*
      options_ptr = nullptr;

  if (route_resp.per_shard_options_size() > 0) {
    options_ptr = &route_resp.per_shard_options(0).options();
  } else if (route_resp.target_node_addresses_size() > 0) {
    auto* o = synthesized_options.Add();
    o->set_node_id(route_resp.target_node_ids_size() > 0
                       ? route_resp.target_node_ids(0)
                       : 0);
    o->set_node_address(route_resp.target_node_addresses(0));
    o->set_is_primary(true);
    options_ptr = &synthesized_options;
  } else {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                         "RouteQuery returned no shards");
  }

  const auto& options = *options_ptr;
  using StubT = std::remove_pointer_t<decltype(dial_fn(std::string{}))>;

  auto adapter = [&](grpc::ClientContext* ctx,
                      const std::string& addr) -> grpc::Status {
    StubT* stub = dial_fn(addr);
    if (!stub) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                           "could not dial target");
    }
    return call_fn(ctx, stub);
  };

  auto result = CallWithReplicaFallback(options, total_deadline,
                                         operation_label, adapter);
  return result.final_status;
}

}  // namespace network
}  // namespace gvdb

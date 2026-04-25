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
// `total_attempt_budget` is the input the helper divides by the number
// of candidates to derive a per-attempt deadline (then clamped to
// [kReplicaFallbackMinPerAttempt, kReplicaFallbackMaxPerAttempt]). It is
// NOT a wall-clock total: a budget of 5s with 5 candidates yields five
// 1-second attempts (=5s wall), but a budget of 5s with 2 candidates
// yields two 2-second attempts (=4s wall), and 30s with 5 candidates
// yields five 2-second attempts (=10s wall, capped). Budgets exist to
// shape per-attempt deadlines, not to enforce a fixed wall-clock cap;
// callers that need a wall-clock cap should bound their own deadline.
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
    std::chrono::milliseconds total_attempt_budget,
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

  std::chrono::milliseconds per_attempt = total_attempt_budget / options.size();
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

    // call_fn is a callable that we may invoke multiple times during
    // fallback iteration; do NOT std::forward it here (that would cast
    // to rvalue on every call, which is fine for stateless lambdas but
    // would silently move-from any captured state on the first call and
    // dangle on subsequent ones for callers that pass &&-qualified
    // operator() callables). Plain lvalue invocation matches the loop
    // semantic: "may be called once per candidate".
    grpc::Status status = call_fn(&context, opt.node_address());
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

// Per-call deadline for the RouteQuery RPC fired inside
// RouteReadAndCallWithFallback. RouteQuery is in-process metadata
// lookup on the coordinator (sub-millisecond healthy); a generous-but-
// bounded ceiling here protects the whole proxy path from a coordinator
// pause / packet drop / GC stall, so reads fail fast instead of hanging
// while the user keeps clicking.
inline constexpr std::chrono::milliseconds kRouteQueryRpcDeadline{1000};

// Convenience wrapper for read paths: pairs a RouteQuery RPC with
// CallWithReplicaFallback. Always sets prefer_routable_replica=true on
// the request — writes go through a different (primary-only) path and
// must not use this helper. Iterates the FIRST shard's options because
// today's proxy read paths assume one logical shard owns the request;
// multi-shard fan-out is the query-node's job.
//
// dial_fn signature:   (const std::string& target_address) -> StubT*
// call_fn signature:   (grpc::ClientContext*, StubT*) -> grpc::Status
//
// `total_attempt_budget` has the same shape-per-attempt semantic as in
// CallWithReplicaFallback (NOT a wall-clock cap); see its docstring.
//
// Returns OK iff at least one candidate succeeded; otherwise the final
// (transient or non-transient) status from the helper.
template <typename DialFn, typename CallFn>
grpc::Status RouteReadAndCallWithFallback(
    proto::internal::InternalService::Stub* internal_client,
    const std::string& collection_name,
    const std::string& operation_label,
    std::chrono::milliseconds total_attempt_budget,
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
  // Bounded deadline so a paused / unreachable coordinator can't stall
  // every proxy read indefinitely.
  route_ctx.set_deadline(std::chrono::system_clock::now() +
                          kRouteQueryRpcDeadline);
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

  auto result = CallWithReplicaFallback(options, total_attempt_budget,
                                         operation_label, adapter);
  return result.final_status;
}

}  // namespace network
}  // namespace gvdb

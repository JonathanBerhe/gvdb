// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "rpc/replica_fallback.h"

#include <thread>
#include <utility>

#include "common/logger.h"
#include "common/metrics.h"
#include "rpc/primary_term_header.h"

namespace gvdb::rpc {

bool IsTransientReplicaError(grpc::StatusCode code) {
  // UNAVAILABLE: the target's gRPC channel reported it was unreachable
  // (connection refused, RST, name-resolution failure). Another node on
  // its own channel is independently dialed and may succeed.
  //
  // DEADLINE_EXCEEDED: the per-attempt deadline elapsed before the
  // target responded. Slow node, GC pause, head-of-line blocking: all
  // node-local issues, so a different node is worth trying.
  //
  // ABORTED: a write raced a primary swap. The data-node returns
  // ABORTED with detail "stale_primary_term" or "not_primary_for_shard"
  // when its local PrimaryTermTracker rejects a write that the proxy
  // routed using stale RouteQuery cache. The proxy must re-route via
  // a fresh RouteQuery: handled by RouteWriteAndCallWithFallback,
  // which re-issues RouteQuery on each retry rather than reusing a
  // stale options list.
  //
  // Codes intentionally NOT considered transient (do not broaden this
  // set without re-examining each):
  //
  //   - INTERNAL / UNKNOWN: a server-side bug or invariant break. The
  //     same code path on another replica would hit the same bug; retry
  //     would only multiply load.
  //   - NOT_FOUND: collection / shard / segment is genuinely missing
  //     in cluster metadata. No replica has it.
  //   - INVALID_ARGUMENT / FAILED_PRECONDITION: caller-side error;
  //     re-sending the same arguments to another replica produces the
  //     same error.
  //   - PERMISSION_DENIED / UNAUTHENTICATED: auth state is global, not
  //     per-replica.
  //   - RESOURCE_EXHAUSTED: rate limit or quota; trying another node
  //     might "work" but would defeat the rate-limiter and amplify
  //     load. Let the caller back off.
  //   - CANCELLED: the upstream caller gave up. We must not initiate
  //     more work on their behalf.
  return code == grpc::StatusCode::UNAVAILABLE || code == grpc::StatusCode::DEADLINE_EXCEEDED ||
         code == grpc::StatusCode::ABORTED;
}

ReplicaFallbackResult CallWithReplicaFallback(
    const google::protobuf::RepeatedPtrField<proto::internal::RouteQueryNodeOption>& options,
    std::chrono::milliseconds total_attempt_budget, const std::string& operation_label,
    ReplicaCallFn call_fn) {
  ReplicaFallbackResult result;

  if (options.empty()) {
    common::Logger::Instance().Warn(
        "replica_fallback[{}]: empty candidate list; surfacing FAILED_PRECONDITION",
        operation_label);
    result.final_status =
        grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "no routable nodes for shard");
    return result;
  }

  std::chrono::milliseconds per_attempt = total_attempt_budget / options.size();
  if (per_attempt < kReplicaFallbackMinPerAttempt) {
    per_attempt = kReplicaFallbackMinPerAttempt;
  }
  if (per_attempt > kReplicaFallbackMaxPerAttempt) {
    per_attempt = kReplicaFallbackMaxPerAttempt;
  }

  grpc::Status last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "no attempts made");

  auto& metrics = common::MetricsRegistry::Instance();

  for (int i = 0; i < options.size(); ++i) {
    const auto& opt = options[i];

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + per_attempt);

    common::Logger::Instance().Debug(
        "replica_fallback[{}]: attempt {}/{} target=node_{} addr={} primary={}", operation_label,
        result.attempts + 1, options.size(), opt.node_id(), opt.node_address(), opt.is_primary());

    grpc::Status status = call_fn(&context, opt.node_address());
    ++result.attempts;
    last_status = status;

    if (status.ok()) {
      result.final_status = status;
      result.used_replica_fallback = (i > 0);
      metrics.RecordReadAttempts(operation_label, result.attempts);
      if (result.used_replica_fallback) {
        const std::string reason =
            options[0].is_primary() ? "primary_unreachable" : "replica_unreachable";
        metrics.IncReadReplicaFallback(operation_label, reason);
        common::Logger::Instance().Info(
            "replica_fallback[{}]: succeeded on candidate {}/{} "
            "(node_{}, reason={}, attempts={})",
            operation_label, i + 1, options.size(), opt.node_id(), reason, result.attempts);
      }
      return result;
    }

    // Non-transient: another replica won't fix the problem, surface now.
    if (!IsTransientReplicaError(status.error_code())) {
      common::Logger::Instance().Debug(
          "replica_fallback[{}]: non-transient status {} on node_{}; "
          "short-circuiting (every replica would return the same error)",
          operation_label, std::to_underlying(status.error_code()), opt.node_id());
      result.final_status = status;
      metrics.RecordReadAttempts(operation_label, result.attempts);
      return result;
    }

    common::Logger::Instance().Debug(
        "replica_fallback[{}]: transient status {} on node_{}; trying next candidate",
        operation_label, std::to_underlying(status.error_code()), opt.node_id());
  }

  // Every candidate was tried and every attempt was a transient failure.
  common::Logger::Instance().Warn(
      "replica_fallback[{}]: exhausted all {} candidate(s); "
      "last status code={} message=\"{}\"",
      operation_label, options.size(), std::to_underlying(last_status.error_code()),
      last_status.error_message());
  result.final_status = last_status;
  metrics.RecordReadAttempts(operation_label, result.attempts);
  metrics.IncReadExhaustedReplicas(operation_label);
  return result;
}

grpc::Status RouteReadAndCallWithFallback(proto::internal::InternalService::Stub* internal_client,
                                          const std::string& collection_name,
                                          const std::string& operation_label,
                                          std::chrono::milliseconds total_attempt_budget,
                                          DialFn dial_fn, StubCallFn call_fn) {
  if (!internal_client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no coordinator client to call RouteQuery");
  }

  proto::internal::RouteQueryRequest route_req;
  route_req.set_collection_name(collection_name);
  route_req.set_prefer_routable_replica(true);

  proto::internal::RouteQueryResponse route_resp;
  grpc::ClientContext route_ctx;
  // Bounded deadline so a paused / unreachable coordinator can't stall
  // every proxy read indefinitely.
  route_ctx.set_deadline(std::chrono::system_clock::now() + kRouteQueryRpcDeadline);
  auto route_status = internal_client->RouteQuery(&route_ctx, route_req, &route_resp);
  if (!route_status.ok()) {
    common::Logger::Instance().Warn("replica_fallback[{}]: RouteQuery failed: {}", operation_label,
                                    route_status.error_message());
    return route_status;
  }
  if (route_resp.per_shard_options_size() == 0) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "RouteQuery returned no shards");
  }

  const auto& options = route_resp.per_shard_options(0).options();

  auto adapter = [&](grpc::ClientContext* ctx, const std::string& addr) -> grpc::Status {
    proto::VectorDBService::Stub* stub = dial_fn(addr);
    if (!stub) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "could not dial target");
    }
    return call_fn(ctx, stub);
  };

  auto result = CallWithReplicaFallback(options, total_attempt_budget, operation_label, adapter);
  return result.final_status;
}

grpc::Status RouteWriteAndCallWithFallback(proto::internal::InternalService::Stub* internal_client,
                                           const std::string& collection_name,
                                           const std::string& operation_label,
                                           std::chrono::milliseconds per_attempt_deadline,
                                           DialFn dial_fn, StubCallFn call_fn) {
  if (!internal_client) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no coordinator client to call RouteQuery");
  }

  auto& metrics = common::MetricsRegistry::Instance();

  grpc::Status last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "no attempts made");

  // attempt 0 = first try, attempts 1..kRouteWriteMaxReroutes = re-route retries.
  for (int attempt = 0; attempt <= kRouteWriteMaxReroutes; ++attempt) {
    proto::internal::RouteQueryRequest route_req;
    route_req.set_collection_name(collection_name);
    // Writes always go to the primary. A draining-but-routable replica
    // is fine for reads but fatal for writes (lost on next swap).
    route_req.set_prefer_routable_replica(false);

    proto::internal::RouteQueryResponse route_resp;
    grpc::ClientContext route_ctx;
    route_ctx.set_deadline(std::chrono::system_clock::now() + kRouteQueryRpcDeadline);
    auto route_status = internal_client->RouteQuery(&route_ctx, route_req, &route_resp);
    if (!route_status.ok()) {
      common::Logger::Instance().Warn("write_fallback[{}]: RouteQuery failed on attempt {}/{}: {}",
                                      operation_label, attempt + 1, kRouteWriteMaxReroutes + 1,
                                      route_status.error_message());
      last_status = route_status;
      // RouteQuery's own error: not retryable here. The coordinator is
      // either down or returning a hard error. Caller can retry the whole
      // request later if they want.
      return last_status;
    }

    // Pick the primary option from the per_shard_options list, which
    // carries the primary_term to stamp on the call.
    std::string target_addr;
    uint64_t primary_term = 0;
    if (route_resp.per_shard_options_size() > 0 &&
        route_resp.per_shard_options(0).options_size() > 0) {
      const auto& opt = route_resp.per_shard_options(0).options(0);
      target_addr = opt.node_address();
      primary_term = opt.primary_term();
    } else {
      last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "RouteQuery returned no target");
      return last_status;
    }

    proto::VectorDBService::Stub* stub = dial_fn(target_addr);
    if (!stub) {
      last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "could not dial primary");
      // Dialing failure is a transient routing problem; re-route.
      common::Logger::Instance().Warn(
          "write_fallback[{}]: dial failed for {}; re-routing (attempt {}/{})", operation_label,
          target_addr, attempt + 1, kRouteWriteMaxReroutes + 1);
      continue;
    }

    grpc::ClientContext call_ctx;
    call_ctx.set_deadline(std::chrono::system_clock::now() + per_attempt_deadline);
    if (primary_term > 0) {
      StampPrimaryTermHeader(&call_ctx, primary_term);
    }

    common::Logger::Instance().Debug("write_fallback[{}]: attempt {}/{} target={} term={}",
                                     operation_label, attempt + 1, kRouteWriteMaxReroutes + 1,
                                     target_addr, primary_term);

    grpc::Status status = call_fn(&call_ctx, stub);
    last_status = status;

    if (status.ok()) {
      if (attempt > 0) {
        // Surfaces in metrics so a primary-swap-induced retry is observable.
        metrics.IncReadReplicaFallback(operation_label, "primary_term_swap");
        common::Logger::Instance().Info(
            "write_fallback[{}]: succeeded after {} re-route(s) (term={})", operation_label,
            attempt, primary_term);
      }
      return status;
    }

    // ABORTED is the canonical "primary swap, re-route" signal.
    // FAILED_PRECONDITION is its sibling from the same write gate: the
    // routed node has no primary record for the shard yet (fresh
    // creation or a node restart racing the coordinator's record push /
    // heartbeat sync); the data-node's own error text says "re-route
    // via RouteQuery". Anything else is non-retryable here: a
    // different primary won't fix a NOT_FOUND or INVALID_ARGUMENT, and
    // UNAVAILABLE on a write is a real availability issue the caller
    // should surface.
    const grpc::StatusCode code = status.error_code();
    if (code != grpc::StatusCode::ABORTED && code != grpc::StatusCode::FAILED_PRECONDITION) {
      common::Logger::Instance().Debug(
          "write_fallback[{}]: non-retryable status code={} on attempt {}; "
          "short-circuiting",
          operation_label, std::to_underlying(code), attempt + 1);
      return status;
    }

    // The missing-record case is cured by time (record push landing),
    // not by routing alone: the route already points at the right
    // node. Give it a short, bounded beat before the next attempt.
    if (code == grpc::StatusCode::FAILED_PRECONDITION) {
      std::this_thread::sleep_for(kWriteUnknownShardBackoff * (attempt + 1));
    }

    common::Logger::Instance().Info(
        "write_fallback[{}]: {} from primary (term={}, msg=\"{}\"); "
        "re-routing via fresh RouteQuery (attempt {}/{})",
        operation_label, code == grpc::StatusCode::ABORTED ? "ABORTED" : "FAILED_PRECONDITION",
        primary_term, status.error_message(), attempt + 1, kRouteWriteMaxReroutes + 1);
  }

  // Exhausted re-routes. Either a multi-swap storm or a primary record
  // that hasn't landed within our bounded backoff; surface the last
  // status and let the upstream client retry.
  common::Logger::Instance().Warn(
      "write_fallback[{}]: exhausted {} re-route(s); last status code={} "
      "message=\"{}\"",
      operation_label, kRouteWriteMaxReroutes, std::to_underlying(last_status.error_code()),
      last_status.error_message());
  return last_status;
}

}  // namespace gvdb::rpc

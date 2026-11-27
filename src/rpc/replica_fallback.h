// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstdint>
#include <string>

#include <google/protobuf/repeated_ptr_field.h>
#include <grpcpp/grpcpp.h>

#include "absl/functional/function_ref.h"
#include "internal.grpc.pb.h"
#include "internal.pb.h"
#include "vectordb.grpc.pb.h"

namespace gvdb::rpc {

// Per-attempt deadline lower bound. A tight overall deadline divided by
// many candidates can produce sub-millisecond budgets; clamp so every
// attempt has enough headroom for a real round-trip on a healthy node.
inline constexpr std::chrono::milliseconds kReplicaFallbackMinPerAttempt{20};

// Per-attempt deadline upper bound. A generous overall deadline divided
// by few candidates can produce many-second budgets; cap so a single
// dead node can't stall failover by more than a couple of seconds.
inline constexpr std::chrono::milliseconds kReplicaFallbackMaxPerAttempt{2000};

// Outcome of a CallWithReplicaFallback invocation. final_status is OK iff
// at least one option succeeded; on a non-OK return, attempts records how
// many options were tried before bailing (1 if a non-transient status
// short-circuited, options.size() if every transient retry was exhausted).
struct ReplicaFallbackResult {
  grpc::Status final_status;
  uint32_t attempts = 0;
  bool used_replica_fallback = false;
};

// Returns true iff the gRPC status is the kind of transient routing
// failure another candidate node might satisfy (target unreachable,
// per-attempt deadline elapsed). Semantic failures (NOT_FOUND, INTERNAL,
// INVALID_ARGUMENT, ...) recur on every node and short-circuit the loop.
bool IsTransientReplicaError(grpc::StatusCode code);

// One attempt against one candidate: the context carries the attempt
// deadline, and `target_address` is the address of the candidate node.
using ReplicaCallFn = absl::FunctionRef<grpc::Status(grpc::ClientContext* context,
                                                     const std::string& target_address)>;

// Returns the client for a node address, or null when the node cannot be
// dialed.
using DialFn = absl::FunctionRef<proto::VectorDBService::Stub*(const std::string& target_address)>;

// Sends the request through the client that DialFn returned.
using StubCallFn = absl::FunctionRef<grpc::Status(grpc::ClientContext* context,
                                                  proto::VectorDBService::Stub* stub)>;

// Iterate a per-shard candidate list and attempt the supplied gRPC call
// against each option in order. Returns on the first OK status, on the
// first non-transient error, or after exhausting every option.
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
ReplicaFallbackResult CallWithReplicaFallback(
    const google::protobuf::RepeatedPtrField<proto::internal::RouteQueryNodeOption>& options,
    std::chrono::milliseconds total_attempt_budget, const std::string& operation_label,
    ReplicaCallFn call_fn);

// Per-call deadline for the RouteQuery RPC fired inside
// RouteReadAndCallWithFallback. RouteQuery is in-process metadata
// lookup on the coordinator (sub-millisecond healthy); a generous-but-
// bounded ceiling here protects the whole proxy path from a coordinator
// pause / packet drop / GC stall, so reads fail fast instead of hanging
// while the user keeps clicking.
inline constexpr std::chrono::milliseconds kRouteQueryRpcDeadline{1000};

// Convenience wrapper for read paths: pairs a RouteQuery RPC with
// CallWithReplicaFallback. Always sets prefer_routable_replica=true on
// the request: writes go through a different (primary-only) path and
// must not use this helper. Iterates the FIRST shard's options because
// today's proxy read paths assume one logical shard owns the request;
// multi-shard fan-out is the query-node's job.
//
// `total_attempt_budget` has the same shape-per-attempt semantic as in
// CallWithReplicaFallback (NOT a wall-clock cap); see its docstring.
//
// Returns OK iff at least one candidate succeeded; otherwise the final
// (transient or non-transient) status from the helper.
grpc::Status RouteReadAndCallWithFallback(proto::internal::InternalService::Stub* internal_client,
                                          const std::string& collection_name,
                                          const std::string& operation_label,
                                          std::chrono::milliseconds total_attempt_budget,
                                          DialFn dial_fn, StubCallFn call_fn);

// Cap on RouteQuery re-issuances during a single write call. A primary
// swap that lands during our first attempt costs one re-route; a second
// swap that races our re-route costs another. Two re-routes covers the
// realistic worst case: a cluster swapping a shard's primary three
// times in two RTTs has bigger problems than a write succeeding.
inline constexpr int kRouteWriteMaxReroutes = 2;

// Backoff before a re-routed attempt when the data-node reported
// FAILED_PRECONDITION (no primary record for the shard yet). Unlike an
// ABORTED swap (where the fix is purely re-routing to the new primary),
// the missing-record case needs a beat of wall-clock for the
// coordinator's record push (or the next heartbeat) to land on the node,
// which the routing already correctly targets. Scaled by attempt number:
// 200ms, then 400ms; bounded by kRouteWriteMaxReroutes.
inline constexpr std::chrono::milliseconds kWriteUnknownShardBackoff{200};

// Write-side analogue of RouteReadAndCallWithFallback. Differs in three
// ways:
//
//   1. RouteQuery is issued with `prefer_routable_replica=false`. Writes
//      MUST go to the primary; a stale-but-routable replica would accept
//      the write at a stale term and lose it on the next swap.
//
//   2. The proxy stamps the routed shard's primary_term as the
//      `gvdb-shard-term` gRPC metadata header on each call. The
//      data-node's write gate compares this term against its locally
//      recorded term and bounces a stale-routing write with ABORTED.
//
//   3. On ABORTED (term-mismatch, see vectordb_service.cpp's write
//      gate), the helper re-issues RouteQuery rather than walking the
//      original options list. The swap means the primary genuinely
//      moved; the original options[0] still points at the demoted node
//      and would loop forever. On FAILED_PRECONDITION (same gate: the
//      node has no primary record for the shard yet, e.g. right after
//      collection creation) the helper backs off briefly and re-routes;
//      the routing is usually already correct and the record push
//      just hasn't landed. Both are bounded by kRouteWriteMaxReroutes
//      before surfacing the last status.
//
// `per_attempt_deadline` is applied to every dispatched call (and to
// the RouteQuery RPCs). Unlike the read helper, this is NOT a budget
// divided across candidates: there's only ever one candidate in play
// at a time (the current primary). A fresh deadline is set on every
// attempt's ClientContext.
//
// Returns OK iff a call eventually succeeded; otherwise the last
// observed status. Non-OK returns other than ABORTED /
// FAILED_PRECONDITION short-circuit immediately (a different primary
// won't satisfy a NOT_FOUND or INVALID_ARGUMENT).
grpc::Status RouteWriteAndCallWithFallback(proto::internal::InternalService::Stub* internal_client,
                                           const std::string& collection_name,
                                           const std::string& operation_label,
                                           std::chrono::milliseconds per_attempt_deadline,
                                           DialFn dial_fn, StubCallFn call_fn);

}  // namespace gvdb::rpc

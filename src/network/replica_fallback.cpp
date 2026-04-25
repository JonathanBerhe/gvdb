// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "network/replica_fallback.h"

namespace gvdb {
namespace network {

bool IsTransientReplicaError(grpc::StatusCode code) {
  // UNAVAILABLE: the target's gRPC channel reported it was unreachable
  // (connection refused, RST, name-resolution failure). Another node on
  // its own channel is independently dialed and may succeed.
  //
  // DEADLINE_EXCEEDED: the per-attempt deadline elapsed before the
  // target responded. Slow node, GC pause, head-of-line blocking — all
  // node-local issues, so a different node is worth trying.
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
  //   - ABORTED: optimistic-concurrency conflict; client must retry
  //     the whole operation, not just route to a replica.
  return code == grpc::StatusCode::UNAVAILABLE ||
         code == grpc::StatusCode::DEADLINE_EXCEEDED;
}

}  // namespace network
}  // namespace gvdb

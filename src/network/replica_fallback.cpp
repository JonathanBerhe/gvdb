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
  return code == grpc::StatusCode::UNAVAILABLE ||
         code == grpc::StatusCode::DEADLINE_EXCEEDED;
}

}  // namespace network
}  // namespace gvdb

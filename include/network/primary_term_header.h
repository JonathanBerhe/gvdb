// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstdint>
#include <string>

#include <grpcpp/grpcpp.h>

namespace gvdb {
namespace network {

// gRPC client-metadata header key the proxy stamps on every write RPC.
// Carries the shard's primary_term as observed at routing time, in
// decimal ASCII. The data-node compares it against its locally-recorded
// term in PrimaryTermTracker; mismatch → ABORTED (proxy retries via a
// fresh RouteQuery).
//
// Header keys must be lowercase per gRPC's HTTP/2 binding. Custom
// (non-binary) keys SHOULD NOT use the "-bin" suffix because the value
// is a small integer; ASCII transit is fine and easier to debug from
// curl-style tools.
inline constexpr char kPrimaryTermHeader[] = "gvdb-shard-term";

// Read kPrimaryTermHeader from a server context's client metadata.
// Returns:
//   - has_term=false when the header is absent (pre-1.x client; the
//     write gate's contract is to accept silently to keep rolling
//     upgrades smooth).
//   - has_term=true + term populated on a parseable value.
//   - has_term=true + term=0 + parse_error=true on a malformed value
//     (caller should reject the request — a header that's there but
//     unparseable is a contract violation, not a back-compat case).
struct PrimaryTermFromHeader {
  bool has_term = false;
  bool parse_error = false;
  uint64_t term = 0;
};

PrimaryTermFromHeader ReadPrimaryTermHeader(grpc::ServerContext* context);

// Stamp kPrimaryTermHeader onto a client context. Used by the proxy
// before dispatching a write RPC.
void StampPrimaryTermHeader(grpc::ClientContext* context, uint64_t term);

}  // namespace network
}  // namespace gvdb

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstdint>
#include <shared_mutex>
#include <unordered_map>

namespace gvdb {
namespace cluster {

// PrimaryTermTracker is the per-data-node state that lets writes be
// gated against "am I currently the primary for this shard, at this
// term?" — anchoring write linearizability across primary swaps.
//
// State updates arrive from two channels:
//   1. Heartbeat response: the coordinator pushes the authoritative
//      shard_primaries list every cycle. Steady-state sync; cadence is
//      whatever HeartbeatSender uses (~10s today).
//   2. Two-phase swap RPCs (PausePrimary / PreparePromote): explicit
//      pushes during a planned transfer. Make the swap visible to the
//      data-node within one RPC RTT instead of waiting on the next
//      heartbeat tick.
//
// Both channels call RecordPrimary / RecordNotPrimary; the contract is
// identical and the tracker doesn't care which channel called it.
class PrimaryTermTracker {
 public:
  // Record that this data-node is the primary for `shard_id` at
  // `term`. Idempotent on re-write of the same (shard, term). Rejects
  // term regression: term must be strictly greater than the previously
  // recorded term for this shard. Returns true on success, false if
  // the call would have regressed the term (caller should log).
  bool RecordPrimary(uint32_t shard_id, uint64_t term);

  // Record that this data-node is NOT the primary for `shard_id`. The
  // `last_known_term` parameter records the cluster's current
  // authoritative term so that a subsequent stale-routing write
  // tagged with `term ≤ last_known_term` can still be rejected with a
  // useful error message ("you're at term N, the cluster is at term M").
  // Idempotent. Like RecordPrimary, rejects regressions.
  bool RecordNotPrimary(uint32_t shard_id, uint64_t last_known_term);

  // Decision the write path uses to decide whether to accept an
  // incoming Insert/Upsert/Delete/UpdateMetadata call.
  enum class AcceptDecision {
    // Match: this node is primary for shard at the requested term.
    Accept,
    // We are primary for shard but at a different term. The write
    // raced a swap; the proxy must re-route. → ABORTED on the wire.
    StaleTerm,
    // We are NOT primary for shard (we know about it but were either
    // never primary or were demoted). → ABORTED on the wire.
    NotPrimary,
    // No entry for this shard. The proxy raced past the heartbeat
    // sync, OR the data-node restarted recently and hasn't caught up.
    // Treat as a "definitely-not-primary" but emit a different code
    // (FAILED_PRECONDITION) so observers can distinguish a true racing
    // swap from a missing-state condition that wants attention.
    UnknownShard,
  };

  // Evaluate whether a write tagged with `request_term` should be
  // accepted. Pure: no side effects. Caller maps the decision to a
  // grpc::Status (ABORTED for StaleTerm/NotPrimary, FAILED_PRECONDITION
  // for UnknownShard).
  AcceptDecision EvaluateWrite(uint32_t shard_id, uint64_t request_term) const;

  // Diagnostic accessors for tests + logs. Returns whether an entry
  // exists for this shard, plus its last-known term.
  struct Snapshot {
    bool has_entry;
    bool is_primary;
    uint64_t term;
  };
  Snapshot Get(uint32_t shard_id) const;

 private:
  struct Entry {
    bool is_primary = false;
    uint64_t term = 0;
  };

  mutable std::shared_mutex mu_;
  std::unordered_map<uint32_t, Entry> table_;
};

}  // namespace cluster
}  // namespace gvdb

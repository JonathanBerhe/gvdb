// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/primary_term_tracker.h"

namespace gvdb {
namespace cluster {

bool PrimaryTermTracker::RecordPrimary(uint32_t shard_id, uint64_t term) {
  std::unique_lock lock(mu_);
  auto it = table_.find(shard_id);
  if (it == table_.end()) {
    table_.emplace(shard_id, Entry{/*is_primary=*/true, term});
    return true;
  }
  auto& entry = it->second;
  // Strict monotonic-term rule:
  //   - term < known: stale heartbeat after a fresher one. Reject.
  //   - term == known: only legal if the recorded role also matches
  //     (idempotent re-record). Same-term role flip is contradictory
  //     because each primary swap bumps the term, so two different
  //     roles at the same term would mean two primaries existed
  //     simultaneously — never true at the coordinator. Reject.
  //   - term > known: forward step, update.
  if (term < entry.term) return false;
  if (term == entry.term && !entry.is_primary) return false;
  entry.is_primary = true;
  entry.term = term;
  return true;
}

bool PrimaryTermTracker::RecordNotPrimary(uint32_t shard_id,
                                           uint64_t last_known_term) {
  std::unique_lock lock(mu_);
  auto it = table_.find(shard_id);
  if (it == table_.end()) {
    table_.emplace(shard_id, Entry{/*is_primary=*/false, last_known_term});
    return true;
  }
  auto& entry = it->second;
  // Symmetric rule (see RecordPrimary).
  if (last_known_term < entry.term) return false;
  if (last_known_term == entry.term && entry.is_primary) return false;
  entry.is_primary = false;
  entry.term = last_known_term;
  return true;
}

PrimaryTermTracker::AcceptDecision PrimaryTermTracker::EvaluateWrite(
    uint32_t shard_id, uint64_t request_term) const {
  std::shared_lock lock(mu_);
  auto it = table_.find(shard_id);
  if (it == table_.end()) {
    // No entry yet. The coordinator hasn't told this data-node
    // anything about this shard. Two real cases: (1) data-node just
    // started and the heartbeat hasn't completed a round-trip; (2)
    // proxy has stale/wrong routing pointing at this node for a
    // shard we don't own. Either way, decline — the proxy must
    // re-route via a fresh RouteQuery.
    return AcceptDecision::UnknownShard;
  }
  const auto& e = it->second;
  if (!e.is_primary) {
    return AcceptDecision::NotPrimary;
  }
  if (e.term != request_term) {
    return AcceptDecision::StaleTerm;
  }
  return AcceptDecision::Accept;
}

PrimaryTermTracker::Snapshot PrimaryTermTracker::Get(uint32_t shard_id) const {
  std::shared_lock lock(mu_);
  auto it = table_.find(shard_id);
  if (it == table_.end()) {
    return Snapshot{false, false, 0};
  }
  return Snapshot{true, it->second.is_primary, it->second.term};
}

}  // namespace cluster
}  // namespace gvdb

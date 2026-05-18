// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_CLUSTER_SHARD_WRITE_GATE_H_
#define GVDB_CLUSTER_SHARD_WRITE_GATE_H_

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

#include "core/types.h"

namespace gvdb {
namespace cluster {

// ============================================================================
// ShardWriteGate - per-shard pause-writes fence for backup snapshots
// ============================================================================
//
// The coordinator brackets a per-shard backup with FreezeWrites /
// UnfreezeWrites so segment files do not change under the uploader.
// While a shard is frozen, the data-node's write-path gate
// (VectorDBService::EvaluateWriteGate) returns RESOURCE_EXHAUSTED so
// clients retry once the backup finishes.
//
// Each freeze carries an opaque token chosen by the coordinator. The
// matching UnfreezeWrites must present the same token, so a stale or
// retried unfreeze from a previous backup cannot drop a freeze owned
// by a newer one. A freeze also carries a lease: it auto-expires
// after `lease_ms` even if UnfreezeWrites is never called, which caps
// the blast radius of a coordinator crash mid-backup. The coordinator
// extends the lease by calling FreezeWrites again with the same token.
//
// Thread-safety: every method is thread-safe.
class ShardWriteGate {
 public:
  ShardWriteGate() = default;

  ShardWriteGate(const ShardWriteGate&) = delete;
  ShardWriteGate& operator=(const ShardWriteGate&) = delete;

  // Freeze writes to `shard_id` for the next `lease_ms` milliseconds.
  // Returns true on success. Returns false (without touching state) if
  // a freeze is already active under a different token — the caller
  // should treat that as "another backup is running on this shard".
  // A repeat freeze with the same token refreshes the lease.
  bool Freeze(core::ShardId shard_id, const std::string& token,
              int64_t lease_ms);

  // Unfreeze writes to `shard_id`. Returns true if a freeze was
  // present and the token matched. Returns false if no freeze was
  // present (idempotent on stale unfreezes) OR if the token didn't
  // match the active freeze (rejected — caller's freeze has been
  // superseded by a newer one).
  bool Unfreeze(core::ShardId shard_id, const std::string& token);

  // True iff `shard_id` is currently frozen (lease not expired).
  // Expired entries are lazily evicted on next access.
  [[nodiscard]] bool IsFrozen(core::ShardId shard_id) const;

  // Test helper: clear every freeze without token checks.
  void ClearForTesting();

 private:
  struct Entry {
    std::string token;
    std::chrono::steady_clock::time_point expires_at;
  };

  // Erase the entry if its lease has expired. Caller holds the mutex.
  // Returns true iff the entry was erased.
  bool MaybeExpire(uint16_t key,
                   std::chrono::steady_clock::time_point now) const;

  mutable std::mutex mu_;
  // Mutable so const IsFrozen can lazily evict.
  mutable std::unordered_map<uint16_t, Entry> frozen_;
};

}  // namespace cluster
}  // namespace gvdb

#endif  // GVDB_CLUSTER_SHARD_WRITE_GATE_H_

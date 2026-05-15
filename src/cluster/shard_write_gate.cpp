// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/shard_write_gate.h"

namespace gvdb {
namespace cluster {

namespace {
constexpr int64_t kMinLeaseMs = 1;
constexpr int64_t kMaxLeaseMs = 60'000;  // 60 s

int64_t ClampLease(int64_t lease_ms) {
  if (lease_ms < kMinLeaseMs) return kMinLeaseMs;
  if (lease_ms > kMaxLeaseMs) return kMaxLeaseMs;
  return lease_ms;
}
}  // namespace

bool ShardWriteGate::MaybeExpire(
    uint16_t key, std::chrono::steady_clock::time_point now) const {
  auto it = frozen_.find(key);
  if (it == frozen_.end()) return false;
  if (now >= it->second.expires_at) {
    frozen_.erase(it);
    return true;
  }
  return false;
}

bool ShardWriteGate::Freeze(core::ShardId shard_id, const std::string& token,
                            int64_t lease_ms) {
  if (token.empty()) return false;
  const auto key = core::ToUInt16(shard_id);
  const auto now = std::chrono::steady_clock::now();
  const auto deadline = now + std::chrono::milliseconds(ClampLease(lease_ms));

  std::lock_guard<std::mutex> lk(mu_);
  MaybeExpire(key, now);

  auto it = frozen_.find(key);
  if (it == frozen_.end()) {
    frozen_[key] = Entry{token, deadline};
    return true;
  }
  if (it->second.token != token) {
    // A different backup owns this shard's freeze right now.
    return false;
  }
  // Same token: extend the lease (refresh).
  it->second.expires_at = deadline;
  return true;
}

bool ShardWriteGate::Unfreeze(core::ShardId shard_id,
                              const std::string& token) {
  if (token.empty()) return false;
  const auto key = core::ToUInt16(shard_id);
  std::lock_guard<std::mutex> lk(mu_);
  auto it = frozen_.find(key);
  if (it == frozen_.end()) return false;
  if (it->second.token != token) return false;
  frozen_.erase(it);
  return true;
}

bool ShardWriteGate::IsFrozen(core::ShardId shard_id) const {
  const auto key = core::ToUInt16(shard_id);
  const auto now = std::chrono::steady_clock::now();
  std::lock_guard<std::mutex> lk(mu_);
  MaybeExpire(key, now);
  return frozen_.find(key) != frozen_.end();
}

void ShardWriteGate::ClearForTesting() {
  std::lock_guard<std::mutex> lk(mu_);
  frozen_.clear();
}

}  // namespace cluster
}  // namespace gvdb

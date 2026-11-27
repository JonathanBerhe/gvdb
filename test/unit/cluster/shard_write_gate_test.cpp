// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <chrono>
#include <cstdint>
#include <thread>

#include "cluster/datanode/shard_write_gate.h"

namespace {

using namespace gvdb;
using namespace gvdb::cluster;

// A lease longer than the ctest timeout of this binary, so a freeze that a
// case expects to be live cannot expire during the case, however slow the
// machine is. It is the longest lease the gate accepts.
constexpr int64_t kLongLeaseMs = 60'000;

// Blocks until the steady clock reaches `deadline`. ShardWriteGate reads the
// steady clock directly, so a lease can only expire by letting real time
// pass. Waiting for a deadline taken after Freeze() returned, instead of a
// fixed sleep, makes sure that the lease has expired.
void SleepUntil(std::chrono::steady_clock::time_point deadline) {
  while (std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_until(deadline);
  }
}

TEST_CASE("ShardWriteGate: a second token cannot steal a live freeze") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(1);

  CHECK(gate.Freeze(s, "tok-A", kLongLeaseMs));
  CHECK_FALSE(gate.Freeze(s, "tok-B", kLongLeaseMs));
  // The original freeze is still in place.
  CHECK(gate.IsFrozen(s));
  // The B token cannot unfreeze either.
  CHECK_FALSE(gate.Unfreeze(s, "tok-B"));
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "tok-A"));
}

TEST_CASE("ShardWriteGate: same-token Freeze refreshes the lease") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(2);

  // Short initial lease so we can observe the refresh.
  CHECK(gate.Freeze(s, "tok", 30));
  // The original lease ends 30 ms after Freeze() read the clock, so no
  // later than this.
  const auto original_lease_end = std::chrono::steady_clock::now() + std::chrono::milliseconds(30);
  // Refresh well before expiry.
  CHECK(gate.Freeze(s, "tok", kLongLeaseMs));
  SleepUntil(original_lease_end);
  // The original 30 ms lease has expired by now; the refreshed lease must
  // keep the shard frozen.
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "tok"));
}

TEST_CASE("ShardWriteGate: expired freeze auto-evicts on next access") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(3);

  CHECK(gate.Freeze(s, "tok", /*lease_ms=*/10));
  // The lease ends 10 ms after Freeze() read the clock, so no later than
  // this.
  SleepUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds(10));
  // IsFrozen lazily evicts the expired entry.
  CHECK_FALSE(gate.IsFrozen(s));
  // A new freeze under a different token must succeed because the
  // previous freeze has expired.
  CHECK(gate.Freeze(s, "tok-2", kLongLeaseMs));
  CHECK(gate.IsFrozen(s));
}

TEST_CASE("ShardWriteGate: rejects empty token") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(4);

  CHECK_FALSE(gate.Freeze(s, "", kLongLeaseMs));
  CHECK_FALSE(gate.IsFrozen(s));
  CHECK(gate.Freeze(s, "real", kLongLeaseMs));
  CHECK_FALSE(gate.Unfreeze(s, ""));
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "real"));
}

TEST_CASE("ShardWriteGate: per-shard isolation") {
  ShardWriteGate gate;
  auto a = core::MakeShardId(10);
  auto b = core::MakeShardId(11);

  CHECK(gate.Freeze(a, "tok-a", kLongLeaseMs));
  CHECK_FALSE(gate.IsFrozen(b));
  // Different shard can be frozen under a different token without
  // affecting shard a.
  CHECK(gate.Freeze(b, "tok-b", kLongLeaseMs));
  CHECK(gate.IsFrozen(a));
  CHECK(gate.IsFrozen(b));
  CHECK(gate.Unfreeze(a, "tok-a"));
  CHECK_FALSE(gate.IsFrozen(a));
  CHECK(gate.IsFrozen(b));
}

}  // namespace

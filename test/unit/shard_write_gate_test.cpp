// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <chrono>
#include <thread>

#include "cluster/shard_write_gate.h"

using namespace gvdb;
using namespace gvdb::cluster;

TEST_CASE("ShardWriteGate: freeze then unfreeze") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(7);

  CHECK_FALSE(gate.IsFrozen(s));
  CHECK(gate.Freeze(s, "tok-1", /*lease_ms=*/5000));
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "tok-1"));
  CHECK_FALSE(gate.IsFrozen(s));
}

TEST_CASE("ShardWriteGate: a second token cannot steal a live freeze") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(1);

  CHECK(gate.Freeze(s, "tok-A", 5000));
  CHECK_FALSE(gate.Freeze(s, "tok-B", 5000));
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
  // Refresh well before expiry.
  CHECK(gate.Freeze(s, "tok", 5000));
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  // The original 30 ms lease would have expired by now; the refreshed
  // 5 s lease must keep the shard frozen.
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "tok"));
}

TEST_CASE("ShardWriteGate: expired freeze auto-evicts on next access") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(3);

  CHECK(gate.Freeze(s, "tok", /*lease_ms=*/10));
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  // IsFrozen lazily evicts the expired entry.
  CHECK_FALSE(gate.IsFrozen(s));
  // A new freeze under a different token must succeed because the
  // previous freeze has expired.
  CHECK(gate.Freeze(s, "tok-2", 1000));
  CHECK(gate.IsFrozen(s));
}

TEST_CASE("ShardWriteGate: rejects empty token") {
  ShardWriteGate gate;
  auto s = core::MakeShardId(4);

  CHECK_FALSE(gate.Freeze(s, "", 1000));
  CHECK_FALSE(gate.IsFrozen(s));
  CHECK(gate.Freeze(s, "real", 1000));
  CHECK_FALSE(gate.Unfreeze(s, ""));
  CHECK(gate.IsFrozen(s));
  CHECK(gate.Unfreeze(s, "real"));
}

TEST_CASE("ShardWriteGate: per-shard isolation") {
  ShardWriteGate gate;
  auto a = core::MakeShardId(10);
  auto b = core::MakeShardId(11);

  CHECK(gate.Freeze(a, "tok-a", 5000));
  CHECK_FALSE(gate.IsFrozen(b));
  // Different shard can be frozen under a different token without
  // affecting shard a.
  CHECK(gate.Freeze(b, "tok-b", 5000));
  CHECK(gate.IsFrozen(a));
  CHECK(gate.IsFrozen(b));
  CHECK(gate.Unfreeze(a, "tok-a"));
  CHECK_FALSE(gate.IsFrozen(a));
  CHECK(gate.IsFrozen(b));
}

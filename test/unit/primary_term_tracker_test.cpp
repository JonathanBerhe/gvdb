// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/primary_term_tracker.h"

#include <doctest/doctest.h>

using namespace gvdb::cluster;
using D = PrimaryTermTracker::AcceptDecision;

TEST_CASE("PrimaryTermTracker: bare tracker returns UnknownShard for any write") {
  PrimaryTermTracker t;
  CHECK(t.EvaluateWrite(0, 0) == D::UnknownShard);
  CHECK(t.EvaluateWrite(7, 100) == D::UnknownShard);
}

TEST_CASE("PrimaryTermTracker: RecordPrimary then matching write Accepts") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 5));
  CHECK(t.EvaluateWrite(0, 5) == D::Accept);
}

TEST_CASE("PrimaryTermTracker: write at older term returns StaleTerm") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 5));
  CHECK(t.EvaluateWrite(0, 4) == D::StaleTerm);
  // Sanity: write at the future term that hasn't been recorded yet is
  // ALSO StaleTerm — local view says "I'm primary at term 5", and a
  // request claiming term 6 must be rejected (it would let a misbehaving
  // proxy bypass the heartbeat sync).
  CHECK(t.EvaluateWrite(0, 6) == D::StaleTerm);
}

TEST_CASE("PrimaryTermTracker: RecordPrimary is idempotent at same term") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 5));
  CHECK(t.RecordPrimary(0, 5));  // re-record same term: ok
  CHECK(t.EvaluateWrite(0, 5) == D::Accept);
}

TEST_CASE("PrimaryTermTracker: RecordPrimary rejects term regression") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 10));
  // Stale heartbeat landing after a fresher one: refuse to walk back.
  CHECK_FALSE(t.RecordPrimary(0, 5));
  // The earlier (term=10) state must still be in effect.
  CHECK(t.EvaluateWrite(0, 10) == D::Accept);
  CHECK(t.EvaluateWrite(0, 5) == D::StaleTerm);
}

TEST_CASE("PrimaryTermTracker: RecordNotPrimary makes future writes NotPrimary") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 5));
  CHECK(t.RecordNotPrimary(0, 6));
  CHECK(t.EvaluateWrite(0, 6) == D::NotPrimary);
  CHECK(t.EvaluateWrite(0, 5) == D::NotPrimary);
}

TEST_CASE("PrimaryTermTracker: RecordNotPrimary advances last-known term") {
  PrimaryTermTracker t;
  CHECK(t.RecordNotPrimary(0, 8));
  // Now any later RecordPrimary at term ≤ 8 is rejected (regression).
  CHECK_FALSE(t.RecordPrimary(0, 8));
  CHECK_FALSE(t.RecordPrimary(0, 7));
  // But a forward step is fine — same node may be re-promoted later.
  CHECK(t.RecordPrimary(0, 9));
  CHECK(t.EvaluateWrite(0, 9) == D::Accept);
}

TEST_CASE("PrimaryTermTracker: distinct shards are independent") {
  PrimaryTermTracker t;
  CHECK(t.RecordPrimary(0, 5));
  CHECK(t.RecordPrimary(1, 99));
  CHECK(t.EvaluateWrite(0, 5) == D::Accept);
  CHECK(t.EvaluateWrite(1, 99) == D::Accept);
  CHECK(t.EvaluateWrite(0, 99) == D::StaleTerm);
  CHECK(t.EvaluateWrite(1, 5) == D::StaleTerm);
}

TEST_CASE("PrimaryTermTracker: Get snapshot reflects state") {
  PrimaryTermTracker t;
  {
    auto s = t.Get(42);
    CHECK_FALSE(s.has_entry);
  }
  CHECK(t.RecordPrimary(42, 7));
  {
    auto s = t.Get(42);
    CHECK(s.has_entry);
    CHECK(s.is_primary);
    CHECK(s.term == 7u);
  }
  CHECK(t.RecordNotPrimary(42, 8));
  {
    auto s = t.Get(42);
    CHECK(s.has_entry);
    CHECK_FALSE(s.is_primary);
    CHECK(s.term == 8u);
  }
}

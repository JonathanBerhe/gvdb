// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Bounded polling for tests that wait for background work. Use it instead
// of a fixed sleep: it returns as soon as the condition holds, and the
// timeout only limits how long a broken run waits.

#pragma once

#include <chrono>
#include <thread>

namespace gvdb::test {

// Calls `done` until it returns true or `timeout` passes. Returns the last
// result, so a caller can REQUIRE(WaitFor(...)).
template <typename Predicate>
bool WaitFor(Predicate done, std::chrono::milliseconds timeout = std::chrono::seconds(10),
             std::chrono::milliseconds interval = std::chrono::milliseconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (done()) return true;
    std::this_thread::sleep_for(interval);
  }
  return done();
}

}  // namespace gvdb::test

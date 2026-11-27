// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <memory>
#include "core/status.h"
#include "core/types.h"

namespace gvdb::consensus {

// Timestamp Oracle (TSO) - Provides globally unique, monotonically increasing timestamps
// for total ordering of operations in a distributed system.
//
// Thread-safe: All methods are safe for concurrent access.
class TimestampOracle {
 public:
  TimestampOracle();
  ~TimestampOracle() = default;

  // Disable copy and move
  TimestampOracle(const TimestampOracle&) = delete;
  TimestampOracle& operator=(const TimestampOracle&) = delete;

  // Get next timestamp (monotonically increasing)
  // Returns a unique timestamp that is guaranteed to be greater than all previous ones
  core::Timestamp GetTimestamp();

 private:
  std::atomic<core::Timestamp> timestamp_{0};
};

}  // namespace gvdb::consensus

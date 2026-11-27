// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/timestamp_oracle.h"
#include "common/logger.h"
#include <chrono>

namespace gvdb {
namespace consensus {

TimestampOracle::TimestampOracle() {
  // Initialize with current time in microseconds for uniqueness across restarts
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count();

  timestamp_.store(static_cast<core::Timestamp>(micros), std::memory_order_release);

  common::Logger::Instance().Info("TimestampOracle initialized with base timestamp: {}",
                                 timestamp_.load());
}

core::Timestamp TimestampOracle::GetTimestamp() {
  return timestamp_.fetch_add(1, std::memory_order_acq_rel);
}

} // namespace consensus
} // namespace gvdb
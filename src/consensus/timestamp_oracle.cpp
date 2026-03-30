// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "consensus/timestamp_oracle.h"
#include "utils/logger.h"
#include <chrono>

namespace gvdb {
namespace consensus {

TimestampOracle::TimestampOracle() {
  // Initialize with current time in microseconds for uniqueness across restarts
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
      now.time_since_epoch()).count();

  timestamp_.store(static_cast<core::Timestamp>(micros), std::memory_order_release);

  utils::Logger::Instance().Info("TimestampOracle initialized with base timestamp: {}",
                                 timestamp_.load());
}

core::Timestamp TimestampOracle::GetTimestamp() {
  return timestamp_.fetch_add(1, std::memory_order_acq_rel);
}

core::Timestamp TimestampOracle::AllocateBatch(size_t count) {
  if (count == 0) {
    return timestamp_.load(std::memory_order_acquire);
  }

  std::lock_guard<std::mutex> lock(mutex_);
  core::Timestamp base = timestamp_.fetch_add(count, std::memory_order_acq_rel);

  utils::Logger::Instance().Debug("Allocated timestamp batch: [{}, {})",
                                  base, base + count);
  return base;
}

core::Timestamp TimestampOracle::CurrentTimestamp() const {
  return timestamp_.load(std::memory_order_acquire);
}

void TimestampOracle::Reset(core::Timestamp ts) {
  std::lock_guard<std::mutex> lock(mutex_);
  timestamp_.store(ts, std::memory_order_release);

  utils::Logger::Instance().Warn("TimestampOracle reset to: {}", ts);
}

bool TimestampOracle::UpdateIfHigher(core::Timestamp ts) {
  core::Timestamp current = timestamp_.load(std::memory_order_acquire);

  while (ts > current) {
    if (timestamp_.compare_exchange_weak(current, ts,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      utils::Logger::Instance().Info("Updated timestamp from {} to {}", current, ts);
      return true;
    }
    // current was updated by compare_exchange_weak, retry
  }

  return false;
}

} // namespace consensus
} // namespace gvdb
// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <utility>

namespace gvdb::common {

// ============================================================================
// Timer - RAII-style timer for measuring elapsed time
// ============================================================================
class Timer {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;
  // Returns the current time. Tests pass a fake clock to measure a known
  // duration without sleeping.
  using Clock = std::function<TimePoint()>;

  // Start the timer
  explicit Timer(Clock clock = [] { return std::chrono::steady_clock::now(); })
      : clock_(std::move(clock)), start_(clock_()) {}

  // Get elapsed time in microseconds
  [[nodiscard]] int64_t ElapsedMicros() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(clock_() - start_).count();
  }

  // Get elapsed time in milliseconds
  [[nodiscard]] int64_t ElapsedMillis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(clock_() - start_).count();
  }

 private:
  Clock clock_;
  TimePoint start_;
};

}  // namespace gvdb::common

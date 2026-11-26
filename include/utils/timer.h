// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_UTILS_TIMER_H_
#define GVDB_UTILS_TIMER_H_

#include <chrono>
#include <cstdint>

namespace gvdb {
namespace utils {

// ============================================================================
// Timer - RAII-style timer for measuring elapsed time
// ============================================================================
class Timer {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = std::chrono::time_point<Clock>;

  // Start the timer
  Timer() : start_(Clock::now()) {}

  // Get elapsed time in microseconds
  [[nodiscard]] int64_t elapsed_micros() const {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               Clock::now() - start_)
        .count();
  }

  // Get elapsed time in milliseconds
  [[nodiscard]] int64_t elapsed_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               Clock::now() - start_)
        .count();
  }

 private:
  TimePoint start_;
};

}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_TIMER_H_
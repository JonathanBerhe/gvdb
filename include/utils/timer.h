#ifndef GVDB_UTILS_TIMER_H_
#define GVDB_UTILS_TIMER_H_

#include <chrono>
#include <functional>

namespace gvdb {
namespace utils {

// ============================================================================
// Timer - RAII-style timer for measuring elapsed time
// ============================================================================
class Timer {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = std::chrono::time_point<Clock>;
  using Duration = std::chrono::nanoseconds;

  // Start the timer
  Timer() : start_(Clock::now()) {}

  // Reset the timer
  void reset() { start_ = Clock::now(); }

  // Get elapsed time in nanoseconds
  [[nodiscard]] int64_t elapsed_nanos() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now() - start_)
        .count();
  }

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

  // Get elapsed time in seconds
  [[nodiscard]] double elapsed_seconds() const {
    return std::chrono::duration<double>(Clock::now() - start_).count();
  }

 private:
  TimePoint start_;
};

// ============================================================================
// ScopedTimer - RAII timer that calls a callback on destruction
// ============================================================================
class ScopedTimer {
 public:
  using Callback = std::function<void(int64_t /*elapsed_micros*/)>;

  explicit ScopedTimer(Callback callback)
      : callback_(std::move(callback)), timer_() {}

  ~ScopedTimer() {
    if (callback_) {
      callback_(timer_.elapsed_micros());
    }
  }

  // Disable copy and move
  ScopedTimer(const ScopedTimer&) = delete;
  ScopedTimer& operator=(const ScopedTimer&) = delete;
  ScopedTimer(ScopedTimer&&) = delete;
  ScopedTimer& operator=(ScopedTimer&&) = delete;

 private:
  Callback callback_;
  Timer timer_;
};

// ============================================================================
// Utility function to measure execution time
// ============================================================================
template <typename F>
int64_t measure_micros(F&& func) {
  Timer timer;
  func();
  return timer.elapsed_micros();
}

template <typename F>
double measure_seconds(F&& func) {
  Timer timer;
  func();
  return timer.elapsed_seconds();
}

}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_TIMER_H_

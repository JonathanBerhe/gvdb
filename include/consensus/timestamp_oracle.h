#pragma once

#include "core/types.h"
#include "core/status.h"
#include <atomic>
#include <memory>
#include <mutex>

namespace gvdb {
namespace consensus {

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

  // Allocate a batch of timestamps for efficient batch operations
  // Returns the base timestamp; caller can use [base, base+count) range
  core::Timestamp AllocateBatch(size_t count);

  // Get current timestamp without incrementing
  core::Timestamp CurrentTimestamp() const;

  // Reset the oracle to a specific timestamp (used for recovery)
  // WARNING: Only use during initialization or snapshot recovery
  void Reset(core::Timestamp ts);

  // Update the timestamp if the given value is higher (for distributed sync)
  // Returns true if the timestamp was updated
  bool UpdateIfHigher(core::Timestamp ts);

 private:
  static constexpr size_t kDefaultBatchSize = 1000;

  std::atomic<core::Timestamp> timestamp_{0};
  mutable std::mutex mutex_;  // For batch allocation
};

} // namespace consensus
} // namespace gvdb

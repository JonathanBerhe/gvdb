#pragma once

#include "core/types.h"
#include "index/index_factory.h"
#include "absl/status/status.h"
#include <memory>
#include <queue>
#include <mutex>

namespace gvdb {
namespace cluster {

// Task for building indexes
struct BuildTask {
  core::SegmentId segment_id;
  core::IndexType index_type;
  uint64_t priority;

  bool operator<(const BuildTask& other) const {
    return priority < other.priority;  // Higher priority first
  }
};

// Data node handles index building and compaction
class DataNode {
 public:
  explicit DataNode(std::unique_ptr<index::IndexFactory> index_factory);
  ~DataNode() = default;

  // Index building
  absl::Status ScheduleBuildTask(const BuildTask& task);
  absl::Status BuildIndex(core::SegmentId segment_id, core::IndexType index_type);

  // Task queue management
  size_t GetPendingTaskCount() const;
  bool HasPendingTasks() const;

  // Compaction (placeholder for Phase 1)
  absl::Status CompactSegments(const std::vector<core::SegmentId>& segments);

 private:
  std::unique_ptr<index::IndexFactory> index_factory_;

  mutable std::mutex queue_mutex_;
  std::priority_queue<BuildTask> build_queue_;
};

}  // namespace cluster
}  // namespace gvdb

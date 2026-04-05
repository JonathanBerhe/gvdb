// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include "core/types.h"
#include "index/index_factory.h"
#include "storage/segment_manager.h"
#include "absl/status/status.h"
#include <atomic>
#include <condition_variable>
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
  DataNode(std::unique_ptr<index::IndexFactory> index_factory,
           std::shared_ptr<storage::SegmentManager> segment_manager);
  ~DataNode() = default;

  // Index building
  absl::Status ScheduleBuildTask(const BuildTask& task);
  absl::Status BuildIndex(core::SegmentId segment_id, core::IndexType index_type);

  // Process all pending build tasks. Returns number of tasks processed.
  size_t ProcessBuildQueue();

  // Run the build loop in a dedicated thread. Blocks until shutdown is set.
  void RunBuildLoop(const std::atomic<bool>& shutdown);

  // Task queue management
  size_t GetPendingTaskCount() const;
  bool HasPendingTasks() const;

  // Compaction: merge multiple sealed segments into one
  absl::Status CompactSegments(const std::vector<core::SegmentId>& segments);

 private:
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::SegmentManager> segment_manager_;

  mutable std::mutex queue_mutex_;
  std::condition_variable build_cv_;
  std::priority_queue<BuildTask> build_queue_;
};

}  // namespace cluster
}  // namespace gvdb

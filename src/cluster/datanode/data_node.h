// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include "absl/status/status.h"
#include "core/types.h"
#include "index/index_factory.h"
#include "storage/segment/segment_store.h"

namespace gvdb::cluster {

// Task for building indexes
struct BuildTask {
  core::SegmentId segment_id;
  core::IndexType index_type;
  uint64_t priority;

  bool operator<(const BuildTask& other) const {
    return priority < other.priority;  // Higher priority first
  }
};

// Data node handles index building
class DataNode {
 public:
  DataNode(std::unique_ptr<index::IndexFactory> index_factory,
           std::shared_ptr<storage::ISegmentStore> segment_store);
  ~DataNode() = default;

  // Index building
  absl::Status ScheduleBuildTask(const BuildTask& task);
  absl::Status BuildIndex(core::SegmentId segment_id, core::IndexType index_type);

  // Process all pending build tasks. Returns number of tasks processed.
  size_t ProcessBuildQueue();

  // Run the build loop in a dedicated thread. Blocks until shutdown is set.
  void RunBuildLoop(const std::atomic<bool>& shutdown);

  // Run TTL sweep loop in a dedicated thread. Sweeps expired vectors every 30s.
  void RunTTLSweepLoop(const std::atomic<bool>& shutdown);

  // Task queue management
  size_t GetPendingTaskCount() const;
  bool HasPendingTasks() const;

 private:
  std::unique_ptr<index::IndexFactory> index_factory_;
  std::shared_ptr<storage::ISegmentStore> segment_store_;

  mutable std::mutex queue_mutex_;
  std::condition_variable build_cv_;
  std::priority_queue<BuildTask> build_queue_;
};

}  // namespace gvdb::cluster

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/datanode/data_node.h"
#include "core/config.h"
#include "common/logger.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace cluster {

DataNode::DataNode(std::unique_ptr<index::IndexFactory> index_factory,
                   std::shared_ptr<storage::ISegmentStore> segment_store)
    : index_factory_(std::move(index_factory)),
      segment_store_(std::move(segment_store)) {
  common::Logger::Instance().Info("DataNode initialized");
}

absl::Status DataNode::ScheduleBuildTask(const BuildTask& task) {
  {
    std::lock_guard lock(queue_mutex_);
    build_queue_.push(task);
  }
  build_cv_.notify_one();

  common::Logger::Instance().Info("Scheduled build task for segment {}, priority: {}",
                                 core::ToUInt32(task.segment_id),
                                 task.priority);

  return absl::OkStatus();
}

absl::Status DataNode::BuildIndex(core::SegmentId segment_id, core::IndexType index_type) {
  auto* segment = segment_store_->GetSegment(segment_id);
  if (!segment) {
    return absl::NotFoundError(
        absl::StrCat("Segment not found: ", core::ToUInt32(segment_id)));
  }

  if (segment->GetState() != core::SegmentState::GROWING) {
    return absl::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(segment_id),
                     " is not in GROWING state (current: ",
                     static_cast<int>(segment->GetState()), ")"));
  }

  if (segment->GetVectorCount() == 0) {
    return absl::FailedPreconditionError(
        absl::StrCat("Segment ", core::ToUInt32(segment_id), " is empty"));
  }

  // Resolve AUTO to a concrete index type + adaptive parameters
  auto config = core::ResolveAutoIndexConfig(
      index_type, segment->GetVectorCount(),
      segment->GetDimension(), segment->GetMetric());

  common::Logger::Instance().Info(
      "Building {} index for segment {} ({} vectors, dim={})",
      static_cast<int>(config.index_type), core::ToUInt32(segment_id),
      segment->GetVectorCount(), segment->GetDimension());

  auto status = segment_store_->SealSegment(segment_id, config);
  if (!status.ok()) {
    common::Logger::Instance().Error(
        "Failed to build index for segment {}: {}",
        core::ToUInt32(segment_id), status.message());
    return status;
  }

  // Persist the sealed segment. On the tiered manager this schedules the async
  // upload to object storage; on the plain manager it writes to local disk.
  // A local flush failure is a durability problem worth surfacing, but the
  // index is built and the segment is already serving queries, so we log it
  // rather than fail the build task (upload retries are tracked separately).
  auto flush_status = segment_store_->FlushSegment(segment_id);
  if (!flush_status.ok()) {
    common::Logger::Instance().Error(
        "Failed to flush segment {} after seal: {}",
        core::ToUInt32(segment_id), flush_status.message());
  }

  common::Logger::Instance().Info(
      "Index built for segment {} (type={}, vectors={})",
      core::ToUInt32(segment_id), static_cast<int>(config.index_type),
      segment->GetVectorCount());

  return absl::OkStatus();
}

size_t DataNode::ProcessBuildQueue() {
  size_t processed = 0;

  while (true) {
    BuildTask task;
    {
      std::lock_guard lock(queue_mutex_);
      if (build_queue_.empty()) break;
      task = build_queue_.top();
      build_queue_.pop();
    }

    auto status = BuildIndex(task.segment_id, task.index_type);
    if (status.ok()) {
      processed++;
    } else {
      common::Logger::Instance().Error(
          "Build task failed for segment {}: {}",
          core::ToUInt32(task.segment_id), status.message());
    }
  }

  return processed;
}

void DataNode::RunBuildLoop(const std::atomic<bool>& shutdown) {
  common::Logger::Instance().Info("Build loop started");
  while (!shutdown.load(std::memory_order_relaxed)) {
    {
      std::unique_lock lock(queue_mutex_);
      build_cv_.wait_for(lock, std::chrono::seconds(5), [this, &shutdown] {
        return shutdown.load(std::memory_order_relaxed) || !build_queue_.empty();
      });
    }
    ProcessBuildQueue();
  }
  // Drain remaining tasks on shutdown
  ProcessBuildQueue();
  common::Logger::Instance().Info("Build loop stopped");
}

void DataNode::RunTTLSweepLoop(const std::atomic<bool>& shutdown) {
  segment_store_->RunTTLSweepLoop(shutdown);
}

size_t DataNode::GetPendingTaskCount() const {
  std::lock_guard lock(queue_mutex_);
  return build_queue_.size();
}

bool DataNode::HasPendingTasks() const {
  std::lock_guard lock(queue_mutex_);
  return !build_queue_.empty();
}

}  // namespace cluster
}  // namespace gvdb

// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "cluster/data_node.h"
#include "utils/logger.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace cluster {

DataNode::DataNode(std::unique_ptr<index::IndexFactory> index_factory,
                   std::shared_ptr<storage::SegmentManager> segment_manager)
    : index_factory_(std::move(index_factory)),
      segment_manager_(std::move(segment_manager)) {
  utils::Logger::Instance().Info("DataNode initialized");
}

absl::Status DataNode::ScheduleBuildTask(const BuildTask& task) {
  {
    std::lock_guard lock(queue_mutex_);
    build_queue_.push(task);
  }
  build_cv_.notify_one();

  utils::Logger::Instance().Info("Scheduled build task for segment {}, priority: {}",
                                 core::ToUInt32(task.segment_id),
                                 task.priority);

  return absl::OkStatus();
}

absl::Status DataNode::BuildIndex(core::SegmentId segment_id, core::IndexType index_type) {
  auto* segment = segment_manager_->GetSegment(segment_id);
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

  // Build IndexConfig from segment metadata + requested index type
  core::IndexConfig config;
  config.index_type = index_type;
  config.dimension = segment->GetDimension();
  config.metric_type = segment->GetMetric();
  // HNSW, IVF, TurboQuant params use defaults from IndexConfig struct

  utils::Logger::Instance().Info(
      "Building {} index for segment {} ({} vectors, dim={})",
      static_cast<int>(index_type), core::ToUInt32(segment_id),
      segment->GetVectorCount(), segment->GetDimension());

  auto status = segment_manager_->SealSegment(segment_id, config);
  if (!status.ok()) {
    utils::Logger::Instance().Error(
        "Failed to build index for segment {}: {}",
        core::ToUInt32(segment_id), status.message());
    return status;
  }

  utils::Logger::Instance().Info(
      "Index built for segment {} (type={}, vectors={})",
      core::ToUInt32(segment_id), static_cast<int>(index_type),
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
      utils::Logger::Instance().Error(
          "Build task failed for segment {}: {}",
          core::ToUInt32(task.segment_id), status.message());
    }
  }

  return processed;
}

void DataNode::RunBuildLoop(const std::atomic<bool>& shutdown) {
  utils::Logger::Instance().Info("Build loop started");
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
  utils::Logger::Instance().Info("Build loop stopped");
}

void DataNode::RunTTLSweepLoop(const std::atomic<bool>& shutdown) {
  utils::Logger::Instance().Info("TTL sweep loop started");
  while (!shutdown.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::seconds(30));
    if (shutdown.load(std::memory_order_relaxed)) break;

    auto all_seg_ids = segment_manager_->GetAllSegmentIds();
    for (auto seg_id : all_seg_ids) {
      auto* seg = segment_manager_->GetSegment(seg_id);
      if (!seg || seg->GetState() != core::SegmentState::GROWING) continue;
      size_t swept = seg->SweepExpired();
      if (swept > 0) {
        utils::Logger::Instance().Info("TTL sweep: deleted {} expired vectors from segment {}",
                                        swept, core::ToUInt32(seg_id));
      }
    }
  }
  utils::Logger::Instance().Info("TTL sweep loop stopped");
}

size_t DataNode::GetPendingTaskCount() const {
  std::lock_guard lock(queue_mutex_);
  return build_queue_.size();
}

bool DataNode::HasPendingTasks() const {
  std::lock_guard lock(queue_mutex_);
  return !build_queue_.empty();
}

absl::Status DataNode::CompactSegments(const std::vector<core::SegmentId>& segments) {
  if (segments.size() < 2) {
    return absl::InvalidArgumentError("Compaction requires at least 2 segments");
  }

  // Validate all segments exist and are SEALED
  core::CollectionId collection_id;
  core::Dimension dimension = 0;
  core::MetricType metric = core::MetricType::L2;
  core::IndexType index_type = core::IndexType::FLAT;

  for (size_t i = 0; i < segments.size(); ++i) {
    auto* seg = segment_manager_->GetSegment(segments[i]);
    if (!seg) {
      return absl::NotFoundError(
          absl::StrCat("Segment not found: ", core::ToUInt32(segments[i])));
    }
    if (seg->GetState() != core::SegmentState::SEALED &&
        seg->GetState() != core::SegmentState::FLUSHED) {
      return absl::FailedPreconditionError(
          absl::StrCat("Segment ", core::ToUInt32(segments[i]),
                       " must be SEALED or FLUSHED for compaction"));
    }
    if (i == 0) {
      collection_id = seg->GetCollectionId();
      dimension = seg->GetDimension();
      metric = seg->GetMetric();
      index_type = seg->GetIndexType();
    } else {
      if (seg->GetCollectionId() != collection_id) {
        return absl::InvalidArgumentError(
            "All segments must belong to the same collection");
      }
    }
  }

  utils::Logger::Instance().Info(
      "Compacting {} segments for collection {} (dim={}, metric={})",
      segments.size(), core::ToUInt32(collection_id), dimension,
      static_cast<int>(metric));

  // Create new GROWING segment
  auto new_seg_result = segment_manager_->CreateSegment(
      collection_id, dimension, metric);
  if (!new_seg_result.ok()) {
    return new_seg_result.status();
  }
  auto new_seg_id = *new_seg_result;
  auto* new_segment = segment_manager_->GetSegment(new_seg_id);

  // Copy vectors and metadata from all source segments
  size_t total_vectors = 0;
  for (auto seg_id : segments) {
    auto* src = segment_manager_->GetSegment(seg_id);
    if (!src) continue;

    auto all_ids = src->GetAllVectorIds();
    if (all_ids.empty()) continue;

    auto get_result = src->GetVectors(all_ids, true);
    if (get_result.found_ids.empty()) continue;

    // Add vectors with metadata to new segment
    bool has_metadata = false;
    for (const auto& md : get_result.found_metadata) {
      if (!md.empty()) { has_metadata = true; break; }
    }

    core::Status add_status;
    if (has_metadata) {
      add_status = new_segment->AddVectorsWithMetadata(
          get_result.found_vectors, get_result.found_ids, get_result.found_metadata);
    } else {
      add_status = new_segment->AddVectors(
          get_result.found_vectors, get_result.found_ids);
    }
    if (!add_status.ok()) {
      auto drop = segment_manager_->DropSegment(new_seg_id, true);
      (void)drop;
      return add_status;
    }
    total_vectors += get_result.found_ids.size();
  }

  // Seal new segment with index
  core::IndexConfig config;
  config.index_type = index_type;
  config.dimension = dimension;
  config.metric_type = metric;

  auto seal_status = segment_manager_->SealSegment(new_seg_id, config);
  if (!seal_status.ok()) {
    segment_manager_->DropSegment(new_seg_id, true);
    return seal_status;
  }

  // Drop source segments
  for (auto seg_id : segments) {
    auto drop_result = segment_manager_->DropSegment(seg_id, true);
    if (!drop_result.ok()) {
      utils::Logger::Instance().Error(
          "Failed to drop source segment {} during compaction: {}",
          core::ToUInt32(seg_id), drop_result.message());
    }
  }

  utils::Logger::Instance().Info(
      "Compaction complete: {} segments merged into segment {} ({} vectors)",
      segments.size(), core::ToUInt32(new_seg_id), total_vectors);

  return absl::OkStatus();
}

}  // namespace cluster
}  // namespace gvdb

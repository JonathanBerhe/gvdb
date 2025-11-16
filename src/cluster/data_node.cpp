#include "cluster/data_node.h"
#include "utils/logger.h"

namespace gvdb {
namespace cluster {

DataNode::DataNode(std::unique_ptr<index::IndexFactory> index_factory)
    : index_factory_(std::move(index_factory)) {
  utils::Logger::Instance().Info("DataNode initialized");
}

absl::Status DataNode::ScheduleBuildTask(const BuildTask& task) {
  std::lock_guard lock(queue_mutex_);
  build_queue_.push(task);

  utils::Logger::Instance().Info("Scheduled build task for segment {}, priority: {}",
                                 core::ToUInt32(task.segment_id),
                                 task.priority);

  return absl::OkStatus();
}

absl::Status DataNode::BuildIndex(core::SegmentId segment_id, core::IndexType index_type) {
  utils::Logger::Instance().Info("Building {} index for segment {}",
                                 static_cast<int>(index_type),
                                 core::ToUInt32(segment_id));

  // TODO: Implement actual index building
  // This would involve:
  // 1. Load segment data
  // 2. Create index using index_factory_
  // 3. Build index from segment vectors
  // 4. Persist index
  // 5. Update segment metadata

  return absl::UnimplementedError("Index building not implemented");
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
  utils::Logger::Instance().Info("Compacting {} segments", segments.size());

  // TODO: Implement segment compaction
  // This would involve:
  // 1. Load all segments
  // 2. Merge vectors into new segment
  // 3. Build index for merged segment
  // 4. Update metadata
  // 5. Delete old segments

  return absl::UnimplementedError("Segment compaction not implemented");
}

}  // namespace cluster
}  // namespace gvdb

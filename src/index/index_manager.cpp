// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "index/index_manager.h"

#include "index/index_factory.h"

#include <mutex>

namespace gvdb {
namespace index {

core::Status IndexManager::CreateIndex(core::SegmentId segment_id,
                                        const core::IndexConfig& config) {
  std::unique_lock lock(mutex_);

  if (indexes_.find(segment_id) != indexes_.end()) {
    return core::AlreadyExistsError(
        absl::StrCat("Index already exists for segment ",
                     core::ToUInt32(segment_id)));
  }

  IndexFactory factory;
  auto index_result = factory.CreateIndex(config);
  if (!index_result.ok()) {
    return index_result.status();
  }

  indexes_[segment_id] = std::move(index_result.value());
  return core::OkStatus();
}

core::StatusOr<core::IVectorIndex*> IndexManager::GetIndex(
    core::SegmentId segment_id) {
  std::shared_lock lock(mutex_);

  auto it = indexes_.find(segment_id);
  if (it == indexes_.end()) {
    return core::NotFoundError(absl::StrCat("Index not found for segment ",
                                             core::ToUInt32(segment_id)));
  }

  return it->second.get();
}

core::Status IndexManager::RemoveIndex(core::SegmentId segment_id) {
  std::unique_lock lock(mutex_);

  auto it = indexes_.find(segment_id);
  if (it == indexes_.end()) {
    return core::NotFoundError(absl::StrCat("Index not found for segment ",
                                             core::ToUInt32(segment_id)));
  }

  indexes_.erase(it);
  return core::OkStatus();
}

bool IndexManager::HasIndex(core::SegmentId segment_id) const {
  std::shared_lock lock(mutex_);
  return indexes_.find(segment_id) != indexes_.end();
}

size_t IndexManager::GetTotalMemoryUsage() const {
  std::shared_lock lock(mutex_);

  size_t total = 0;
  for (const auto& [_, index] : indexes_) {
    total += index->GetMemoryUsage();
  }
  return total;
}

size_t IndexManager::GetIndexCount() const {
  std::shared_lock lock(mutex_);
  return indexes_.size();
}

void IndexManager::Clear() {
  std::unique_lock lock(mutex_);
  indexes_.clear();
}

}  // namespace index
}  // namespace gvdb
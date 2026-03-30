// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_INDEX_INDEX_MANAGER_H_
#define GVDB_INDEX_INDEX_MANAGER_H_

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "core/config.h"
#include "core/interfaces.h"
#include "core/status.h"
#include "core/types.h"

// Hash function for SegmentId (must be defined before use in unordered_map)
namespace std {
template <>
struct hash<gvdb::core::SegmentId> {
  size_t operator()(const gvdb::core::SegmentId& id) const {
    return std::hash<uint32_t>()(gvdb::core::ToUInt32(id));
  }
};
}  // namespace std

namespace gvdb {
namespace index {

// IndexManager manages the lifecycle of vector indexes
// Provides thread-safe access to multiple indexes
class IndexManager {
 public:
  IndexManager() = default;
  ~IndexManager() = default;

  // Non-copyable and non-movable (due to mutex)
  IndexManager(const IndexManager&) = delete;
  IndexManager& operator=(const IndexManager&) = delete;
  IndexManager(IndexManager&&) = delete;
  IndexManager& operator=(IndexManager&&) = delete;

  // Create and register a new index
  [[nodiscard]] core::Status CreateIndex(
      core::SegmentId segment_id, const core::IndexConfig& config);

  // Get an existing index
  [[nodiscard]] core::StatusOr<core::IVectorIndex*> GetIndex(
      core::SegmentId segment_id);

  // Remove an index
  [[nodiscard]] core::Status RemoveIndex(core::SegmentId segment_id);

  // Check if index exists
  [[nodiscard]] bool HasIndex(core::SegmentId segment_id) const;

  // Get memory usage across all indexes
  [[nodiscard]] size_t GetTotalMemoryUsage() const;

  // Get number of managed indexes
  [[nodiscard]] size_t GetIndexCount() const;

  // Clear all indexes
  void Clear();

 private:
  // Thread-safe index storage
  std::unordered_map<core::SegmentId, std::unique_ptr<core::IVectorIndex>> indexes_;
  mutable std::shared_mutex mutex_;
};

}  // namespace index
}  // namespace gvdb

#endif  // GVDB_INDEX_INDEX_MANAGER_H_
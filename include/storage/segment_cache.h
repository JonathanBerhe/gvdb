// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SEGMENT_CACHE_H_
#define GVDB_STORAGE_SEGMENT_CACHE_H_

#include <list>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/status.h"
#include "core/types.h"

namespace gvdb {
namespace storage {

// ============================================================================
// SegmentCache - LRU disk cache for segments downloaded from object storage
// ============================================================================
// Tracks segment files stored in a local cache directory. Evicts least-recently
// accessed segments when the cache exceeds its configured size limit.
// Thread-safe via shared_mutex.
class SegmentCache {
 public:
  SegmentCache(const std::string& cache_dir, size_t max_cache_bytes);

  // Check if a segment is cached locally
  [[nodiscard]] bool HasSegment(core::SegmentId id) const;

  // Get the local filesystem path for a cached segment directory
  // Returns empty string if not cached.
  [[nodiscard]] std::string GetSegmentPath(core::SegmentId id) const;

  // Register a downloaded segment in the cache (after download completes)
  [[nodiscard]] core::Status RegisterSegment(
      core::SegmentId id, size_t size_bytes);

  // Touch a segment to update its LRU position (most recently used)
  void Touch(core::SegmentId id);

  // Evict least-recently-used segments to free at least needed_bytes.
  // Returns the IDs of evicted segments.
  [[nodiscard]] core::StatusOr<std::vector<core::SegmentId>> Evict(
      size_t needed_bytes);

  // Get total size of all cached segments in bytes
  [[nodiscard]] size_t GetCachedSize() const;

  // Get number of cached segments
  [[nodiscard]] size_t GetCachedCount() const;

  // Get the cache directory path
  [[nodiscard]] const std::string& GetCacheDir() const { return cache_dir_; }

  // Get the max cache size
  [[nodiscard]] size_t GetMaxCacheBytes() const { return max_cache_bytes_; }

 private:
  std::string cache_dir_;
  size_t max_cache_bytes_;
  size_t current_size_ = 0;
  mutable std::shared_mutex mutex_;

  // LRU list: front = most recently used, back = least recently used
  std::list<core::SegmentId> lru_list_;

  struct CacheEntry {
    size_t size_bytes;
    std::list<core::SegmentId>::iterator lru_it;
  };
  std::unordered_map<uint32_t, CacheEntry> entries_;

  // Move segment to front of LRU list (caller must hold write lock)
  void TouchLocked(core::SegmentId id);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SEGMENT_CACHE_H_

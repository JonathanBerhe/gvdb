// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/segment_cache.h"

#include <filesystem>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace storage {

SegmentCache::SegmentCache(const std::string& cache_dir,
                           size_t max_cache_bytes)
    : cache_dir_(cache_dir), max_cache_bytes_(max_cache_bytes) {
  std::filesystem::create_directories(cache_dir_);
}

bool SegmentCache::HasSegment(core::SegmentId id) const {
  std::shared_lock lock(mutex_);
  return entries_.count(core::ToUInt32(id)) > 0;
}

std::string SegmentCache::GetSegmentPath(core::SegmentId id) const {
  std::shared_lock lock(mutex_);
  if (entries_.count(core::ToUInt32(id)) == 0) {
    return "";
  }
  return absl::StrCat(cache_dir_, "/segment_", core::ToUInt32(id));
}

core::Status SegmentCache::RegisterSegment(
    core::SegmentId id, size_t size_bytes) {
  std::unique_lock lock(mutex_);
  auto key = core::ToUInt32(id);

  // If already registered, update size and touch
  auto it = entries_.find(key);
  if (it != entries_.end()) {
    current_size_ -= it->second.size_bytes;
    current_size_ += size_bytes;
    it->second.size_bytes = size_bytes;
    TouchLocked(id);
    return core::OkStatus();
  }

  // Add new entry at front of LRU
  lru_list_.push_front(id);
  entries_[key] = CacheEntry{size_bytes, lru_list_.begin()};
  current_size_ += size_bytes;
  return core::OkStatus();
}

void SegmentCache::Touch(core::SegmentId id) {
  std::unique_lock lock(mutex_);
  TouchLocked(id);
}

void SegmentCache::TouchLocked(core::SegmentId id) {
  auto it = entries_.find(core::ToUInt32(id));
  if (it == entries_.end()) return;

  // Move to front of LRU list
  lru_list_.erase(it->second.lru_it);
  lru_list_.push_front(id);
  it->second.lru_it = lru_list_.begin();
}

core::StatusOr<std::vector<core::SegmentId>> SegmentCache::Evict(
    size_t needed_bytes) {
  std::unique_lock lock(mutex_);
  std::vector<core::SegmentId> evicted;
  size_t freed = 0;

  // Evict from the back (least recently used) until we have enough space
  while (freed < needed_bytes && !lru_list_.empty()) {
    auto victim_id = lru_list_.back();
    auto key = core::ToUInt32(victim_id);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
      lru_list_.pop_back();
      continue;
    }

    freed += it->second.size_bytes;
    current_size_ -= it->second.size_bytes;

    // Delete the cached files from disk
    auto segment_path = absl::StrCat(
        cache_dir_, "/segment_", core::ToUInt32(victim_id));
    std::error_code ec;
    std::filesystem::remove_all(segment_path, ec);

    lru_list_.pop_back();
    entries_.erase(it);
    evicted.push_back(victim_id);
  }

  return evicted;
}

size_t SegmentCache::GetCachedSize() const {
  std::shared_lock lock(mutex_);
  return current_size_;
}

size_t SegmentCache::GetCachedCount() const {
  std::shared_lock lock(mutex_);
  return entries_.size();
}

}  // namespace storage
}  // namespace gvdb

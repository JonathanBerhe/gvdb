#include "utils/query_cache.h"

#include <cstring>
#include <functional>

namespace gvdb {
namespace utils {

// FNV-1a hash for float vectors (fast, good distribution)
static uint64_t HashVector(const float* data, size_t size) {
  uint64_t hash = 14695981039346656037ULL;
  const auto* bytes = reinterpret_cast<const uint8_t*>(data);
  size_t byte_len = size * sizeof(float);
  for (size_t i = 0; i < byte_len; ++i) {
    hash ^= bytes[i];
    hash *= 1099511628211ULL;
  }
  return hash;
}

static uint64_t HashString(const std::string& s) {
  if (s.empty()) return 0;
  return std::hash<std::string>{}(s);
}

CacheKey MakeCacheKey(core::CollectionId collection_id,
                      const float* vector_data, size_t vector_size,
                      uint32_t top_k,
                      const std::string& filter) {
  return CacheKey{
      core::ToUInt32(collection_id),
      HashVector(vector_data, vector_size),
      top_k,
      HashString(filter),
  };
}

QueryCache::QueryCache(size_t max_entries)
    : max_entries_(max_entries) {}

std::optional<core::SearchResult> QueryCache::Get(const CacheKey& key,
                                                   uint32_t collection_id) {
  std::unique_lock lock(mutex_);

  auto it = map_.find(key);
  if (it == map_.end()) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
  }

  // Check version (invalidation)
  auto ver_it = collection_versions_.find(collection_id);
  uint64_t current_version = (ver_it != collection_versions_.end()) ? ver_it->second : 0;
  if (it->second->collection_version != current_version) {
    // Stale entry — evict
    lru_list_.erase(it->second);
    map_.erase(it);
    misses_.fetch_add(1, std::memory_order_relaxed);
    return std::nullopt;
  }

  // Move to front (MRU)
  lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
  hits_.fetch_add(1, std::memory_order_relaxed);
  return it->second->result;
}

void QueryCache::Put(const CacheKey& key, const core::SearchResult& result,
                     uint32_t collection_id) {
  std::unique_lock lock(mutex_);

  auto it = map_.find(key);
  if (it != map_.end()) {
    // Update existing
    it->second->result = result;
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    return;
  }

  // Evict if at capacity
  while (lru_list_.size() >= max_entries_) {
    auto& back = lru_list_.back();
    map_.erase(back.key);
    lru_list_.pop_back();
    evictions_.fetch_add(1, std::memory_order_relaxed);
  }

  // Get current version
  uint64_t version = 0;
  auto ver_it = collection_versions_.find(collection_id);
  if (ver_it != collection_versions_.end()) {
    version = ver_it->second;
  }

  // Insert at front
  lru_list_.push_front(Entry{key, result, version});
  map_[key] = lru_list_.begin();
}

void QueryCache::InvalidateCollection(uint32_t collection_id) {
  std::unique_lock lock(mutex_);
  collection_versions_[collection_id]++;
}

size_t QueryCache::size() const {
  std::shared_lock lock(mutex_);
  return lru_list_.size();
}

}  // namespace utils
}  // namespace gvdb

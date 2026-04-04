#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
#include <optional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "core/types.h"

namespace gvdb {
namespace utils {

struct CacheKey {
  uint32_t collection_id;
  uint64_t vector_hash;
  uint32_t top_k;
  uint64_t filter_hash;

  bool operator==(const CacheKey& other) const {
    return collection_id == other.collection_id &&
           vector_hash == other.vector_hash &&
           top_k == other.top_k &&
           filter_hash == other.filter_hash;
  }
};

struct CacheKeyHash {
  size_t operator()(const CacheKey& k) const {
    size_t h = std::hash<uint32_t>{}(k.collection_id);
    h ^= std::hash<uint64_t>{}(k.vector_hash) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<uint32_t>{}(k.top_k) + 0x9e3779b9 + (h << 6) + (h >> 2);
    h ^= std::hash<uint64_t>{}(k.filter_hash) + 0x9e3779b9 + (h << 6) + (h >> 2);
    return h;
  }
};

// Build a CacheKey from search parameters.
// vector_data/vector_size are used to compute a hash of the query vector.
CacheKey MakeCacheKey(core::CollectionId collection_id,
                      const float* vector_data, size_t vector_size,
                      uint32_t top_k,
                      const std::string& filter = "");

// Thread-safe LRU query result cache.
class QueryCache {
 public:
  explicit QueryCache(size_t max_entries = 10000);

  // Lookup a cached result. Returns nullopt on miss.
  std::optional<core::SearchResult> Get(const CacheKey& key, uint32_t collection_id);

  // Store a result in the cache.
  void Put(const CacheKey& key, const core::SearchResult& result, uint32_t collection_id);

  // Invalidate all entries for a collection (call on insert/delete/drop).
  void InvalidateCollection(uint32_t collection_id);

  // Stats
  size_t size() const;
  uint64_t hits() const { return hits_.load(std::memory_order_relaxed); }
  uint64_t misses() const { return misses_.load(std::memory_order_relaxed); }

 private:
  struct Entry {
    CacheKey key;
    core::SearchResult result;
    uint64_t collection_version;
  };

  using ListIter = std::list<Entry>::iterator;

  mutable std::shared_mutex mutex_;
  std::list<Entry> lru_list_;
  std::unordered_map<CacheKey, ListIter, CacheKeyHash> map_;
  std::unordered_map<uint32_t, uint64_t> collection_versions_;
  size_t max_entries_;

  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> misses_{0};
  std::atomic<uint64_t> evictions_{0};
};

}  // namespace utils
}  // namespace gvdb

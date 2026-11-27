// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace gvdb::core {

// Strong-typed IDs to prevent mixing different ID types
// Using enum class provides type safety and prevents implicit conversions

enum class VectorId : uint64_t {};
enum class CollectionId : uint32_t {};
enum class SegmentId : uint32_t {};
enum class ShardId : uint16_t {};
enum class NodeId : uint32_t {};

// Helper functions for ID conversions and string representation
inline uint64_t ToUInt64(VectorId id) { return static_cast<uint64_t>(id); }
inline uint32_t ToUInt32(CollectionId id) { return static_cast<uint32_t>(id); }
inline uint32_t ToUInt32(SegmentId id) { return static_cast<uint32_t>(id); }
inline uint16_t ToUInt16(ShardId id) { return static_cast<uint16_t>(id); }
inline uint32_t ToUInt32(NodeId id) { return static_cast<uint32_t>(id); }

inline VectorId MakeVectorId(uint64_t value) { return VectorId{value}; }
inline CollectionId MakeCollectionId(uint32_t value) { return CollectionId{value}; }
inline SegmentId MakeSegmentId(uint32_t value) { return SegmentId{value}; }

// The segment that holds one shard of a collection. The coordinator, the
// resolvers and the restore path all derive segment ids this way, so a
// collection has at most 1000 shards.
inline SegmentId ShardSegmentId(CollectionId cid, uint32_t shard_index) {
  return static_cast<SegmentId>(ToUInt32(cid) * 1000 + shard_index);
}
// The shard of a vector in a collection: the vector id modulo the shard
// count. The proxy, the write gate and the write handlers must route a
// vector to the same shard, so they all use this.
inline uint32_t ShardIndexForVector(uint64_t vector_id, uint32_t num_shards) {
  return static_cast<uint32_t>(vector_id % num_shards);
}
inline ShardId MakeShardId(uint16_t value) { return ShardId{value}; }
inline NodeId MakeNodeId(uint32_t value) { return NodeId{value}; }

// Distance metric types for vector similarity search
enum class MetricType {
  L2,             // Euclidean distance
  INNER_PRODUCT,  // Inner product (for normalized vectors, equivalent to cosine)
  COSINE          // Cosine similarity
};

// Index types supported by the system
enum class IndexType {
  FLAT,            // Brute-force exact search
  HNSW,            // Hierarchical Navigable Small World
  IVF_FLAT,        // Inverted File with exhaustive search
  IVF_PQ,          // Inverted File with Product Quantization
  IVF_SQ,          // Inverted File with Scalar Quantization
  TURBOQUANT,      // TurboQuant online quantization (data-oblivious)
  IVF_TURBOQUANT,  // IVF partitioning + TurboQuant compression
  AUTO             // Automatically select based on vector count at seal time
};

// Auto-index tier boundaries (vector count thresholds)
constexpr size_t kAutoTierFlatMax = 10'000;      // <10K → FLAT
constexpr size_t kAutoTierHnswMax = 500'000;     // 10K–500K → HNSW
constexpr size_t kAutoTierSqMax = 1'000'000;     // 500K–1M → IVF_SQ
constexpr size_t kAutoTierTQ4Max = 100'000'000;  // 1M–100M → IVF_TURBOQUANT 4-bit
                                                 // ≥100M → IVF_TURBOQUANT 2-bit

// Resolve AUTO index type to a concrete type based on vector count.
// Returns the input unchanged if it is not AUTO.
inline IndexType ResolveAutoIndexType(IndexType type, size_t vector_count) {
  if (type != IndexType::AUTO) return type;
  if (vector_count < kAutoTierFlatMax) return IndexType::FLAT;
  if (vector_count < kAutoTierHnswMax) return IndexType::HNSW;
  if (vector_count < kAutoTierSqMax) return IndexType::IVF_SQ;
  return IndexType::IVF_TURBOQUANT;
}

// Segment states in the storage lifecycle
enum class SegmentState {
  GROWING,  // Accepting new vectors
  SEALED,   // Read-only, optimized for queries
  FLUSHED,  // Persisted to storage
  DROPPED   // Marked for deletion
};

// Search result entry containing a vector ID and its distance
struct SearchResultEntry {
  VectorId id;
  float distance;

  SearchResultEntry() : id(MakeVectorId(0)), distance(0.0f) {}
  SearchResultEntry(VectorId vec_id, float dist) : id(vec_id), distance(dist) {}
};

// Search result containing multiple entries
struct SearchResult {
  std::vector<SearchResultEntry> entries;

  SearchResult() = default;
  explicit SearchResult(size_t reserve_size) { entries.reserve(reserve_size); }

  void AddEntry(VectorId id, float distance) { entries.emplace_back(id, distance); }

  size_t Size() const { return entries.size(); }
};

// Dimension type for vectors
using Dimension = int32_t;

// Timestamp type for MVCC and TSO
using Timestamp = uint64_t;

// Constants
constexpr CollectionId kInvalidCollectionId = CollectionId{0};
constexpr ShardId kInvalidShardId = ShardId{0};
constexpr NodeId kInvalidNodeId = NodeId{0};

}  // namespace gvdb::core

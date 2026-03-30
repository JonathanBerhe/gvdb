// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_CORE_TYPES_H_
#define GVDB_CORE_TYPES_H_

#include <cstdint>
#include <string>
#include <vector>

namespace gvdb {
namespace core {

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
inline ShardId MakeShardId(uint16_t value) { return ShardId{value}; }
inline NodeId MakeNodeId(uint32_t value) { return NodeId{value}; }

// Distance metric types for vector similarity search
enum class MetricType {
  L2,              // Euclidean distance
  INNER_PRODUCT,   // Inner product (for normalized vectors, equivalent to cosine)
  COSINE           // Cosine similarity
};

// Index types supported by the system
enum class IndexType {
  FLAT,      // Brute-force exact search
  HNSW,      // Hierarchical Navigable Small World
  IVF_FLAT,  // Inverted File with exhaustive search
  IVF_PQ,    // Inverted File with Product Quantization
  IVF_SQ     // Inverted File with Scalar Quantization
};

// Segment states in the storage lifecycle
enum class SegmentState {
  GROWING,   // Accepting new vectors
  SEALED,    // Read-only, optimized for queries
  FLUSHED,   // Persisted to storage
  DROPPED    // Marked for deletion
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
  explicit SearchResult(size_t reserve_size) {
    entries.reserve(reserve_size);
  }

  void AddEntry(VectorId id, float distance) {
    entries.emplace_back(id, distance);
  }

  size_t Size() const { return entries.size(); }
  bool Empty() const { return entries.empty(); }
};

// Dimension type for vectors
using Dimension = int32_t;

// Timestamp type for MVCC and TSO
using Timestamp = uint64_t;

// Constants
constexpr VectorId kInvalidVectorId = VectorId{0};
constexpr CollectionId kInvalidCollectionId = CollectionId{0};
constexpr SegmentId kInvalidSegmentId = SegmentId{0};
constexpr ShardId kInvalidShardId = ShardId{0};
constexpr NodeId kInvalidNodeId = NodeId{0};
constexpr Timestamp kInvalidTimestamp = 0;

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_TYPES_H_
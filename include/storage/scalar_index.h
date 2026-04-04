// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_STORAGE_SCALAR_INDEX_H_
#define GVDB_STORAGE_SCALAR_INDEX_H_

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "core/filter.h"
#include "core/metadata.h"
#include "core/types.h"

namespace gvdb {
namespace storage {

// A bitmap of vector IDs for fast set intersection
using VectorIdSet = std::unordered_set<uint64_t>;

// ScalarIndex — inverted index on a single metadata field
//
// For equality/IN lookups: hash map from value -> set of vector IDs
// For range queries on numeric fields: sorted map for ordered iteration
//
// Built incrementally during GROWING state, or bulk-built during Seal().
class ScalarIndex {
 public:
  ScalarIndex() = default;

  // Add a vector ID -> value mapping
  void Add(uint64_t vector_id, const core::MetadataValue& value);

  // Remove a vector ID from all postings
  void Remove(uint64_t vector_id);

  // Lookup: exact equality match
  VectorIdSet LookupEqual(const core::MetadataValue& value) const;

  // Lookup: range (min, max) for numeric types
  // Returns IDs where min <= field_value <= max
  // Use std::nullopt for open-ended ranges
  VectorIdSet LookupRange(const core::MetadataValue* min_val,
                          const core::MetadataValue* max_val) const;

  // Lookup: IN (set membership)
  VectorIdSet LookupIn(const std::vector<core::MetadataValue>& values) const;

  // Lookup: prefix match for strings (LIKE 'prefix%')
  VectorIdSet LookupPrefix(const std::string& prefix) const;

  // Get total number of indexed entries
  size_t Size() const { return total_entries_; }

  // Get approximate memory usage
  size_t GetMemoryUsage() const;

 private:
  // Hash index: value -> set of vector IDs (for equality, IN)
  std::unordered_map<std::string, VectorIdSet> hash_index_;

  // Sorted index: string key -> set of vector IDs (for range, prefix)
  // Key is a sortable string representation of the value
  std::map<std::string, VectorIdSet> sorted_index_;

  size_t total_entries_ = 0;

  // Convert MetadataValue to a hashable string key
  static std::string ValueToKey(const core::MetadataValue& value);

  // Convert MetadataValue to a sort-friendly key (preserves numeric ordering)
  static std::string ValueToSortKey(const core::MetadataValue& value);
};

// ScalarIndexSet — manages scalar indexes for all indexed fields in a segment
class ScalarIndexSet {
 public:
  ScalarIndexSet() = default;

  // Index a vector's metadata across all fields
  void IndexVector(uint64_t vector_id, const core::Metadata& metadata);

  // Remove a vector from all field indexes
  void RemoveVector(uint64_t vector_id, const core::Metadata& metadata);

  // Build indexes from a full metadata map (used during Seal)
  void BuildFromMetadata(
      const std::unordered_map<uint64_t, core::Metadata>& metadata_map);

  // Evaluate a parsed filter node using indexes
  // Returns the set of matching vector IDs, or std::nullopt if the filter
  // cannot be fully resolved by indexes (requires fallback to scan)
  std::optional<VectorIdSet> Evaluate(const core::FilterNode& filter) const;

  // Check if any indexes exist
  bool HasIndexes() const { return !field_indexes_.empty(); }

  // Get approximate memory usage
  size_t GetMemoryUsage() const;

 private:
  // Per-field scalar indexes
  std::unordered_map<std::string, ScalarIndex> field_indexes_;

  // Get or create index for a field
  ScalarIndex& GetOrCreateIndex(const std::string& field);
};

}  // namespace storage
}  // namespace gvdb

#endif  // GVDB_STORAGE_SCALAR_INDEX_H_

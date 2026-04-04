// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "storage/scalar_index.h"

#include <cmath>
#include <cstring>
#include <limits>
#include <sstream>

#include "core/filter.h"

namespace gvdb {
namespace storage {

// ============================================================================
// ScalarIndex
// ============================================================================

std::string ScalarIndex::ValueToKey(const core::MetadataValue& value) {
  return std::visit([](const auto& v) -> std::string {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, int64_t>) {
      return "i:" + std::to_string(v);
    } else if constexpr (std::is_same_v<T, double>) {
      return "d:" + std::to_string(v);
    } else if constexpr (std::is_same_v<T, std::string>) {
      return "s:" + v;
    } else if constexpr (std::is_same_v<T, bool>) {
      return v ? "b:1" : "b:0";
    }
    return "";
  }, value);
}

std::string ScalarIndex::ValueToSortKey(const core::MetadataValue& value) {
  return std::visit([](const auto& v) -> std::string {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_same_v<T, int64_t>) {
      // Encode int64_t as a fixed-width string that sorts lexicographically
      // Flip sign bit so negative numbers sort before positive
      uint64_t encoded = static_cast<uint64_t>(v) ^ (1ULL << 63);
      char buf[17];
      snprintf(buf, sizeof(buf), "%016llx",
               static_cast<unsigned long long>(encoded));
      return std::string("i:") + buf;
    } else if constexpr (std::is_same_v<T, double>) {
      // IEEE 754 double to sortable bytes
      uint64_t bits;
      std::memcpy(&bits, &v, sizeof(bits));
      if (bits & (1ULL << 63)) {
        bits = ~bits;  // Negative: flip all bits
      } else {
        bits ^= (1ULL << 63);  // Positive: flip sign bit
      }
      char buf[17];
      snprintf(buf, sizeof(buf), "%016llx",
               static_cast<unsigned long long>(bits));
      return std::string("d:") + buf;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return "s:" + v;
    } else if constexpr (std::is_same_v<T, bool>) {
      return v ? "b:1" : "b:0";
    }
    return "";
  }, value);
}

void ScalarIndex::Add(uint64_t vector_id, const core::MetadataValue& value) {
  auto key = ValueToKey(value);
  hash_index_[key].insert(vector_id);

  auto sort_key = ValueToSortKey(value);
  sorted_index_[sort_key].insert(vector_id);

  total_entries_++;
}

void ScalarIndex::Remove(uint64_t vector_id) {
  // Remove from all postings (O(n) but infrequent)
  for (auto& [key, ids] : hash_index_) {
    if (ids.erase(vector_id)) {
      total_entries_--;
    }
  }
  for (auto& [key, ids] : sorted_index_) {
    ids.erase(vector_id);
  }
}

VectorIdSet ScalarIndex::LookupEqual(const core::MetadataValue& value) const {
  auto key = ValueToKey(value);
  auto it = hash_index_.find(key);
  if (it != hash_index_.end()) {
    return it->second;
  }
  return {};
}

VectorIdSet ScalarIndex::LookupRange(const core::MetadataValue* min_val,
                                      const core::MetadataValue* max_val) const {
  VectorIdSet result;

  auto begin = sorted_index_.begin();
  auto end = sorted_index_.end();

  if (min_val) {
    auto min_key = ValueToSortKey(*min_val);
    begin = sorted_index_.lower_bound(min_key);
  }
  if (max_val) {
    auto max_key = ValueToSortKey(*max_val);
    // Include the max value itself (upper_bound gives us past-the-end)
    end = sorted_index_.upper_bound(max_key);
  }

  for (auto it = begin; it != end; ++it) {
    result.insert(it->second.begin(), it->second.end());
  }
  return result;
}

VectorIdSet ScalarIndex::LookupIn(
    const std::vector<core::MetadataValue>& values) const {
  VectorIdSet result;
  for (const auto& value : values) {
    auto key = ValueToKey(value);
    auto it = hash_index_.find(key);
    if (it != hash_index_.end()) {
      result.insert(it->second.begin(), it->second.end());
    }
  }
  return result;
}

VectorIdSet ScalarIndex::LookupPrefix(const std::string& prefix) const {
  VectorIdSet result;
  std::string search_prefix = "s:" + prefix;
  for (auto it = sorted_index_.lower_bound(search_prefix);
       it != sorted_index_.end(); ++it) {
    if (it->first.compare(0, search_prefix.size(), search_prefix) != 0) {
      break;  // Past the prefix range
    }
    result.insert(it->second.begin(), it->second.end());
  }
  return result;
}

size_t ScalarIndex::GetMemoryUsage() const {
  size_t usage = sizeof(*this);
  for (const auto& [key, ids] : hash_index_) {
    usage += key.size() + ids.size() * sizeof(uint64_t) + 64;
  }
  for (const auto& [key, ids] : sorted_index_) {
    usage += key.size() + ids.size() * sizeof(uint64_t) + 64;
  }
  return usage;
}

// ============================================================================
// ScalarIndexSet
// ============================================================================

ScalarIndex& ScalarIndexSet::GetOrCreateIndex(const std::string& field) {
  return field_indexes_[field];
}

void ScalarIndexSet::IndexVector(uint64_t vector_id,
                                  const core::Metadata& metadata) {
  for (const auto& [field, value] : metadata) {
    GetOrCreateIndex(field).Add(vector_id, value);
  }
}

void ScalarIndexSet::RemoveVector(uint64_t vector_id,
                                   const core::Metadata& metadata) {
  for (const auto& [field, value] : metadata) {
    auto it = field_indexes_.find(field);
    if (it != field_indexes_.end()) {
      it->second.Remove(vector_id);
    }
  }
}

void ScalarIndexSet::BuildFromMetadata(
    const std::unordered_map<uint64_t, core::Metadata>& metadata_map) {
  field_indexes_.clear();
  for (const auto& [vector_id, metadata] : metadata_map) {
    IndexVector(vector_id, metadata);
  }
}

std::optional<VectorIdSet> ScalarIndexSet::Evaluate(
    const core::FilterNode& filter) const {
  // Try to resolve the filter using scalar indexes
  // Returns std::nullopt if the filter references unindexed fields

  if (auto* comp = dynamic_cast<const core::ComparisonNode*>(&filter)) {
    auto it = field_indexes_.find(comp->field());
    if (it == field_indexes_.end()) {
      return std::nullopt;  // Field not indexed
    }

    const auto& idx = it->second;
    switch (comp->op()) {
      case core::ComparisonOp::EQUAL:
        return idx.LookupEqual(comp->value());
      case core::ComparisonOp::NOT_EQUAL: {
        // NOT_EQUAL: get all, subtract equal
        // Fallback to scan for NOT_EQUAL (expensive to compute from index)
        return std::nullopt;
      }
      case core::ComparisonOp::LESS_THAN:
        return idx.LookupRange(nullptr, &comp->value());
      case core::ComparisonOp::LESS_EQUAL:
        return idx.LookupRange(nullptr, &comp->value());
      case core::ComparisonOp::GREATER_THAN:
        return idx.LookupRange(&comp->value(), nullptr);
      case core::ComparisonOp::GREATER_EQUAL:
        return idx.LookupRange(&comp->value(), nullptr);
      case core::ComparisonOp::LIKE: {
        if (auto* str_val = std::get_if<std::string>(&comp->value())) {
          // Only handle prefix LIKE (pattern%)
          if (str_val->size() > 0 && str_val->back() == '%') {
            std::string prefix = str_val->substr(0, str_val->size() - 1);
            return idx.LookupPrefix(prefix);
          }
        }
        return std::nullopt;
      }
      default:
        return std::nullopt;
    }
  }

  if (auto* in_node = dynamic_cast<const core::InNode*>(&filter)) {
    auto it = field_indexes_.find(in_node->field());
    if (it == field_indexes_.end()) {
      return std::nullopt;
    }
    if (in_node->is_negated()) {
      return std::nullopt;  // NOT IN is expensive, fallback
    }
    return it->second.LookupIn(in_node->values());
  }

  if (auto* logical = dynamic_cast<const core::LogicalNode*>(&filter)) {
    if (logical->op() == core::LogicalOp::AND) {
      auto left_result = Evaluate(logical->left());
      auto right_result = Evaluate(logical->right());

      if (left_result && right_result) {
        // Intersect both sets
        VectorIdSet result;
        const auto& smaller = left_result->size() < right_result->size()
                                  ? *left_result : *right_result;
        const auto& larger = left_result->size() < right_result->size()
                                 ? *right_result : *left_result;
        for (uint64_t id : smaller) {
          if (larger.count(id)) {
            result.insert(id);
          }
        }
        return result;
      }
      // If one side resolved, return it (the other side will be post-filtered)
      if (left_result) return left_result;
      if (right_result) return right_result;
      return std::nullopt;
    }

    if (logical->op() == core::LogicalOp::OR) {
      auto left_result = Evaluate(logical->left());
      auto right_result = Evaluate(logical->right());

      if (left_result && right_result) {
        // Union both sets
        VectorIdSet result = *left_result;
        result.insert(right_result->begin(), right_result->end());
        return result;
      }
      // OR requires both sides to be resolved by index
      return std::nullopt;
    }

    if (logical->op() == core::LogicalOp::NOT) {
      // NOT is expensive with indexes, fallback to scan
      return std::nullopt;
    }
  }

  return std::nullopt;
}

size_t ScalarIndexSet::GetMemoryUsage() const {
  size_t usage = sizeof(*this);
  for (const auto& [field, idx] : field_indexes_) {
    usage += field.size() + idx.GetMemoryUsage();
  }
  return usage;
}

}  // namespace storage
}  // namespace gvdb

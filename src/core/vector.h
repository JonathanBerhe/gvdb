// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <memory>
#include <vector>

#include "core/status.h"
#include "core/types.h"

namespace gvdb {
namespace core {

// Custom allocator for SIMD-aligned memory
// Ensures vector data is aligned to 32-byte boundaries for AVX operations
template <typename T, size_t Alignment = 32>
class AlignedAllocator {
 public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using reference = T&;
  using const_reference = const T&;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  template <typename U>
  struct rebind {
    using other = AlignedAllocator<U, Alignment>;
  };

  AlignedAllocator() noexcept = default;

  template <typename U>
  AlignedAllocator(const AlignedAllocator<U, Alignment>&) noexcept {}

  pointer allocate(size_type n);
  void deallocate(pointer p, size_type n) noexcept;

  template <typename U>
  bool operator==(const AlignedAllocator<U, Alignment>&) const noexcept {
    return true;
  }

  template <typename U>
  bool operator!=(const AlignedAllocator<U, Alignment>&) const noexcept {
    return false;
  }
};

// Vector class with SIMD-aligned memory
class Vector {
 public:
  // Constructor: creates a vector with specified dimension
  // All elements are initialized to 0.0
  explicit Vector(Dimension dim);

  // Constructor: creates a vector from existing data
  Vector(Dimension dim, const float* data);

  // Constructor: creates a vector from std::vector
  explicit Vector(const std::vector<float>& data);

  // Move semantics
  Vector(Vector&& other) noexcept;
  Vector& operator=(Vector&& other) noexcept;

  // Copy semantics
  Vector(const Vector& other);
  Vector& operator=(const Vector& other);

  ~Vector() = default;

  // Accessors
  [[nodiscard]] Dimension dimension() const noexcept { return dimension_; }
  [[nodiscard]] float* data() noexcept { return data_.data(); }
  [[nodiscard]] const float* data() const noexcept { return data_.data(); }
  [[nodiscard]] size_t size() const noexcept { return data_.size(); }
  [[nodiscard]] size_t byte_size() const noexcept { return data_.size() * sizeof(float); }

  // Element access
  [[nodiscard]] float& operator[](size_t index) { return data_[index]; }
  [[nodiscard]] const float& operator[](size_t index) const { return data_[index]; }

  // Vector operations
  [[nodiscard]] float Norm() const;

  // Distance calculations
  [[nodiscard]] float L2Distance(const Vector& other) const;
  [[nodiscard]] float InnerProduct(const Vector& other) const;
  [[nodiscard]] float CosineDistance(const Vector& other) const;

 private:
  Dimension dimension_;
  std::vector<float, AlignedAllocator<float, 32>> data_;
};

// Utility functions for vector operations

// Create a zero vector
[[nodiscard]] Vector ZeroVector(Dimension dim);

// Create a random vector (useful for testing)
[[nodiscard]] Vector RandomVector(Dimension dim);

}  // namespace core
}  // namespace gvdb


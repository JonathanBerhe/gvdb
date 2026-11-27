// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "core/vector.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <random>

#include "absl/strings/str_cat.h"

namespace gvdb::core {

// ============================================================================
// AlignedAllocator Implementation
// ============================================================================

template <typename T, size_t Alignment>
typename AlignedAllocator<T, Alignment>::pointer AlignedAllocator<T, Alignment>::allocate(
    size_type n) {
  if (n == 0) {
    return nullptr;
  }

  // Check for overflow
  if (n > std::numeric_limits<size_type>::max() / sizeof(T)) {
    throw std::bad_alloc();
  }

  size_t size = n * sizeof(T);
  void* ptr = nullptr;

#ifdef _WIN32
  ptr = _aligned_malloc(size, Alignment);
#else
  if (posix_memalign(&ptr, Alignment, size) != 0) {
    ptr = nullptr;
  }
#endif

  if (!ptr) {
    throw std::bad_alloc();
  }

  return static_cast<pointer>(ptr);
}

template <typename T, size_t Alignment>
void AlignedAllocator<T, Alignment>::deallocate(pointer p, size_type) noexcept {
  if (p) {
#ifdef _WIN32
    _aligned_free(p);
#else
    free(p);
#endif
  }
}

// Explicit template instantiation for float
template class AlignedAllocator<float, 32>;

// ============================================================================
// Vector Implementation
// ============================================================================

Vector::Vector(Dimension dim) : dimension_(dim), data_(dim, 0.0f) {
  if (dim <= 0) {
    throw std::invalid_argument(absl::StrCat("Vector dimension must be positive, got ", dim));
  }
}

Vector::Vector(Dimension dim, const float* data) : dimension_(dim), data_(dim) {
  if (dim <= 0) {
    throw std::invalid_argument(absl::StrCat("Vector dimension must be positive, got ", dim));
  }
  if (!data) {
    throw std::invalid_argument("Data pointer cannot be null");
  }
  std::memcpy(data_.data(), data, dim * sizeof(float));
}

Vector::Vector(const std::vector<float>& data)
    : dimension_(static_cast<Dimension>(data.size())), data_(data.size()) {
  if (data.empty()) {
    throw std::invalid_argument("Cannot create vector from empty data");
  }
  std::memcpy(data_.data(), data.data(), data.size() * sizeof(float));
}

Vector::Vector(Vector&& other) noexcept
    : dimension_(other.dimension_), data_(std::move(other.data_)) {
  other.dimension_ = 0;
}

Vector& Vector::operator=(Vector&& other) noexcept {
  if (this != &other) {
    dimension_ = other.dimension_;
    data_ = std::move(other.data_);
    other.dimension_ = 0;
  }
  return *this;
}

Vector::Vector(const Vector& other) = default;

Vector& Vector::operator=(const Vector& other) {
  if (this != &other) {
    dimension_ = other.dimension_;
    data_ = other.data_;
  }
  return *this;
}

float Vector::Norm() const {
  float sum = 0.0f;
  for (float i : data_) {
    sum += i * i;
  }
  return std::sqrt(sum);
}

float Vector::L2Distance(const Vector& other) const {
  if (dimension_ != other.dimension_) {
    return -1.0f;  // Invalid
  }

  float sum = 0.0f;
  for (size_t i = 0; i < data_.size(); ++i) {
    float diff = data_[i] - other.data_[i];
    sum += diff * diff;
  }
  return std::sqrt(sum);
}

float Vector::InnerProduct(const Vector& other) const {
  if (dimension_ != other.dimension_) {
    return 0.0f;  // Invalid
  }

  float sum = 0.0f;
  for (size_t i = 0; i < data_.size(); ++i) {
    sum += data_[i] * other.data_[i];
  }
  return sum;
}

float Vector::CosineDistance(const Vector& other) const {
  if (dimension_ != other.dimension_) {
    return 2.0f;  // Invalid (max distance is 2 for cosine)
  }

  float dot_product = InnerProduct(other);
  float norm_a = Norm();
  float norm_b = other.Norm();

  if (norm_a < 1e-8f || norm_b < 1e-8f) {
    return 2.0f;  // Invalid
  }

  float cosine_similarity = dot_product / (norm_a * norm_b);
  // Clamp to [-1, 1] to handle numerical errors
  cosine_similarity = std::max(-1.0f, std::min(1.0f, cosine_similarity));
  return 1.0f - cosine_similarity;
}

// ============================================================================
// Utility Functions
// ============================================================================

Vector ZeroVector(Dimension dim) { return Vector(dim); }

Vector RandomVector(Dimension dim) {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::normal_distribution<float> dist(0.0f, 1.0f);

  Vector vec(dim);
  for (Dimension i = 0; i < dim; ++i) {
    vec[i] = dist(gen);
  }
  return vec;
}

}  // namespace gvdb::core

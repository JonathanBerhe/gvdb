// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_CORE_STATUS_H_
#define GVDB_CORE_STATUS_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"

namespace gvdb {
namespace core {

// Re-export absl status types for consistent usage across the codebase
using Status = absl::Status;

template <typename T>
using StatusOr = absl::StatusOr<T>;

// Convenience function to create OK status
inline Status OkStatus() {
  return absl::OkStatus();
}

// Error creation helpers with consistent formatting
inline Status InvalidArgumentError(absl::string_view message) {
  return absl::InvalidArgumentError(message);
}

inline Status InternalError(absl::string_view message) {
  return absl::InternalError(message);
}

inline Status NotFoundError(absl::string_view message) {
  return absl::NotFoundError(message);
}

inline Status AlreadyExistsError(absl::string_view message) {
  return absl::AlreadyExistsError(message);
}

inline Status ResourceExhaustedError(absl::string_view message) {
  return absl::ResourceExhaustedError(message);
}

inline Status FailedPreconditionError(absl::string_view message) {
  return absl::FailedPreconditionError(message);
}

inline Status OutOfRangeError(absl::string_view message) {
  return absl::OutOfRangeError(message);
}

inline Status UnimplementedError(absl::string_view message) {
  return absl::UnimplementedError(message);
}

inline Status UnavailableError(absl::string_view message) {
  return absl::UnavailableError(message);
}

inline Status DeadlineExceededError(absl::string_view message) {
  return absl::DeadlineExceededError(message);
}

inline Status AbortedError(absl::string_view message) {
  return absl::AbortedError(message);
}

inline Status CancelledError(absl::string_view message) {
  return absl::CancelledError(message);
}

// Macro for checking status and propagating errors
#define RETURN_IF_ERROR(expr)                                       \
  do {                                                              \
    const auto _status = (expr);                                    \
    if (!_status.ok()) {                                            \
      return _status;                                               \
    }                                                               \
  } while (0)

// Macro helpers for unique variable names
#define GVDB_CONCAT_IMPL_(a, b) a##b
#define GVDB_CONCAT_(a, b) GVDB_CONCAT_IMPL_(a, b)

// Macro for extracting value from StatusOr or returning error.
// Uses __LINE__ to generate unique variable names so the macro
// can be used multiple times in the same scope.
#define ASSIGN_OR_RETURN(lhs, expr)                                 \
  auto GVDB_CONCAT_(_statusor_, __LINE__) = (expr);                 \
  if (!GVDB_CONCAT_(_statusor_, __LINE__).ok()) {                   \
    return GVDB_CONCAT_(_statusor_, __LINE__).status();              \
  }                                                                  \
  lhs = std::move(GVDB_CONCAT_(_statusor_, __LINE__).value())

}  // namespace core
}  // namespace gvdb

#endif  // GVDB_CORE_STATUS_H_
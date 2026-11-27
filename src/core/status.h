// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// GVDB reports errors with absl::Status and absl::StatusOr. This header
// includes both and adds two macros that pass an error to the caller.

#pragma once

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

#define GVDB_STATUS_CONCAT_INNER(a, b) a##b
#define GVDB_STATUS_CONCAT(a, b) GVDB_STATUS_CONCAT_INNER(a, b)

// Evaluates `expr`, an absl::Status. When it is not OK, returns it from the
// calling function.
#define RETURN_IF_ERROR(expr)    \
  do {                           \
    const auto _status = (expr); \
    if (!_status.ok()) {         \
      return _status;            \
    }                            \
  } while (0)

// Evaluates `expr`, an absl::StatusOr. When it is not OK, returns its status
// from the calling function. Otherwise moves its value into `lhs`, which can
// be a declaration: ASSIGN_OR_RETURN(auto segment, store.GetSegment(id));
#define ASSIGN_OR_RETURN(lhs, expr) \
  GVDB_ASSIGN_OR_RETURN_IMPL(GVDB_STATUS_CONCAT(_status_or_, __LINE__), lhs, expr)

#define GVDB_ASSIGN_OR_RETURN_IMPL(status_or, lhs, expr) \
  auto status_or = (expr);                               \
  if (!status_or.ok()) return std::move(status_or).status(); \
  lhs = std::move(*status_or)

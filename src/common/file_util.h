// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace gvdb::common {

// The whole content of the file at `path`. NotFound when the file cannot be
// opened, Internal when the read fails.
absl::StatusOr<std::string> ReadFileToString(const std::string& path);

// Writes `contents` to <path>.tmp and then renames it over `path`, so that a
// crash in the middle never leaves a truncated file at `path`.
absl::Status WriteFileAtomically(const std::string& path, const std::string& contents);

}  // namespace gvdb::common

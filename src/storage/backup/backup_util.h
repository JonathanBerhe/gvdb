// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Helpers that the backup and the restore manager share. Private to
// storage/backup.

#pragma once

#include <cstddef>
#include <string>

namespace gvdb {
namespace storage {

// Joins an object key prefix and a key. For S3 targets the prefix is the
// request prefix. A local target roots its FilesystemObjectStore at the
// request path, so its prefix is empty.
std::string BuildKey(const std::string& prefix, const std::string& suffix);

// The current UTC time in RFC 3339 format, for manifests and job ids.
std::string RFC3339Now();

// `len` random lowercase hex characters, for job ids.
std::string RandomToken(size_t len = 6);

// True when `child` resolves to a path under `root` (lexically).
bool PathUnderRoot(const std::string& root, const std::string& child);

}  // namespace storage
}  // namespace gvdb

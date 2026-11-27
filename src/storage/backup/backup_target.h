// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <string>
#include <variant>

namespace gvdb {
namespace storage {

// C++ mirror of the backup proto enums and the BackupTarget oneof. The
// backup and the restore manager share these types.

enum class BackupState {
  PENDING = 0,
  RUNNING = 1,
  COMPLETED = 2,
  FAILED = 3,
  CANCELLED = 4,
};

struct S3BackupTarget {
  std::string bucket;
  std::string prefix;
};

struct LocalBackupTarget {
  std::string path;
};

// Mirrors proto BackupTarget oneof; std::monostate represents "unset".
using BackupTarget =
    std::variant<std::monostate, S3BackupTarget, LocalBackupTarget>;

}  // namespace storage
}  // namespace gvdb

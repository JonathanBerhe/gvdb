// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "common/file_util.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <system_error>

#include "absl/strings/str_cat.h"

namespace gvdb {
namespace common {

absl::StatusOr<std::string> ReadFileToString(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return absl::NotFoundError(absl::StrCat("Cannot open: ", path));
  }
  std::ostringstream contents;
  contents << in.rdbuf();
  if (in.bad()) {
    return absl::InternalError(absl::StrCat("Read failed: ", path));
  }
  return contents.str();
}

absl::Status WriteFileAtomically(const std::string& path, const std::string& contents) {
  const std::string tmp_path = path + ".tmp";
  {
    std::ofstream out(tmp_path, std::ios::trunc);
    if (!out.is_open()) {
      return absl::InternalError(absl::StrCat("Failed to open temp file for write: ", tmp_path));
    }
    out << contents;
    if (!out.good()) {
      return absl::InternalError(absl::StrCat("Failed to write temp file: ", tmp_path));
    }
  }
  std::error_code ec;
  std::filesystem::rename(tmp_path, path, ec);
  if (ec) {
    return absl::InternalError(absl::StrCat("Failed to replace ", path, ": ", ec.message()));
  }
  return absl::OkStatus();
}

}  // namespace common
}  // namespace gvdb

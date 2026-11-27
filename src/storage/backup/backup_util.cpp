// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0
//
// Helpers that the backup and the restore manager share.

#include "storage/backup/backup_util.h"
#include <chrono>
#include <ctime>
#include <filesystem>
#include <random>
#include <system_error>
#include "absl/strings/str_cat.h"

namespace gvdb::storage {

// Compose object keys relative to the IObjectStore's namespace.
// For S3 targets we honor `request.prefix`; for local targets the
// FilesystemObjectStore is rooted at the request's path, so no prefix
// is needed. Either way every key passes through this helper so the
// shape stays uniform.
std::string BuildKey(const std::string& prefix, const std::string& suffix) {
  if (prefix.empty()) return suffix;
  // Strip any trailing slash on prefix; suffix always starts with a slash-free
  // path produced by BackupManifest::*Key().
  std::string p = prefix;
  while (!p.empty() && p.back() == '/') p.pop_back();
  return absl::StrCat(p, "/", suffix);
}

std::string RFC3339Now() {
  auto now = std::chrono::system_clock::now();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
  auto t = static_cast<std::time_t>(secs);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
  return buf;
}

std::string RandomToken(size_t len) {
  static const char kAlphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  thread_local std::mt19937 rng{std::random_device{}()};
  std::uniform_int_distribution<int> dist(0, sizeof(kAlphabet) - 2);
  std::string s(len, 'a');
  for (auto& c : s) c = kAlphabet[dist(rng)];
  return s;
}

// True iff `child` resolves to a path under `root` (lexically).
bool PathUnderRoot(const std::string& root, const std::string& child) {
  if (root.empty()) return false;
  std::error_code ec;
  auto r = std::filesystem::weakly_canonical(std::filesystem::path(root), ec);
  if (ec) r = std::filesystem::path(root);
  auto c = std::filesystem::weakly_canonical(std::filesystem::path(child), ec);
  if (ec) c = std::filesystem::path(child);
  auto rs = r.string();
  auto cs = c.string();
  if (rs.empty() || cs.empty()) return false;
  if (cs.size() < rs.size()) return false;
  if (!cs.starts_with(rs)) return false;
  // Either equal, or the next character is a separator.
  return cs.size() == rs.size() || cs[rs.size()] == '/' || cs[rs.size()] == '\\';
}

}  // namespace gvdb::storage

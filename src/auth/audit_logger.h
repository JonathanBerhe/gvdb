// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace spdlog {
class logger;
}

namespace gvdb {
namespace auth {

// Settings of the audit log file.
struct AuditLogConfig {
  bool enabled = false;
  std::string file_path = "/tmp/gvdb/logs/audit.jsonl";
  size_t max_file_size_mb = 100;
  size_t max_files = 10;
};

// Dedicated audit logger that writes structured JSON lines to a separate file.
// Uses a named spdlog logger ("audit") with its own rotating file sink,
// completely independent from the operational logger.
class AuditLogger {
 public:
  static void Initialize(const AuditLogConfig& config);
  static void Log(const std::string& json_line);
  static bool IsEnabled();
  static void Shutdown();

 private:
  static std::shared_ptr<spdlog::logger> logger_;
  static std::atomic<bool> enabled_;
};

}  // namespace auth
}  // namespace gvdb

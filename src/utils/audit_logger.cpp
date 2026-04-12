// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/audit_logger.h"

#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>

namespace gvdb {
namespace utils {

std::shared_ptr<spdlog::logger> AuditLogger::logger_;
std::atomic<bool> AuditLogger::enabled_{false};

void AuditLogger::Initialize(const AuditLogConfig& config) {
  if (!config.enabled) {
    return;
  }

  // Drop any previous instance (safe for re-initialization in tests)
  spdlog::drop("audit");
  logger_.reset();

  // Ensure parent directory exists
  auto parent = std::filesystem::path(config.file_path).parent_path();
  if (!parent.empty()) {
    std::filesystem::create_directories(parent);
  }

  auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
      config.file_path,
      config.max_file_size_mb * 1024 * 1024,
      config.max_files);

  // Raw message only — timestamp is inside the JSON
  sink->set_pattern("%v");

  logger_ = std::make_shared<spdlog::logger>("audit", sink);
  logger_->set_level(spdlog::level::info);
  logger_->flush_on(spdlog::level::info);  // Flush every entry — audit must not be lost

  spdlog::register_logger(logger_);
  enabled_.store(true);
}

void AuditLogger::Log(const std::string& json_line) {
  if (enabled_.load(std::memory_order_relaxed) && logger_) {
    logger_->info(json_line);
  }
}

bool AuditLogger::IsEnabled() {
  return enabled_.load(std::memory_order_relaxed);
}

void AuditLogger::Shutdown() {
  if (logger_) {
    logger_->flush();
    spdlog::drop("audit");
    logger_.reset();
  }
  enabled_.store(false);
}

}  // namespace utils
}  // namespace gvdb

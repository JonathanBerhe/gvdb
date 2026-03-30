// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/logger.h"

#include <memory>
#include <mutex>

#include "spdlog/spdlog.h"
#include "spdlog/async.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace gvdb {
namespace utils {

// ============================================================================
// Static members
// ============================================================================
std::unique_ptr<Logger> Logger::instance_ = nullptr;

// ============================================================================
// Helper: Convert LogLevel to spdlog::level
// ============================================================================
static spdlog::level::level_enum ToSpdlogLevel(LogLevel level) {
  switch (level) {
    case LogLevel::TRACE:
      return spdlog::level::trace;
    case LogLevel::DEBUG:
      return spdlog::level::debug;
    case LogLevel::INFO:
      return spdlog::level::info;
    case LogLevel::WARN:
      return spdlog::level::warn;
    case LogLevel::ERROR:
      return spdlog::level::err;
    case LogLevel::CRITICAL:
      return spdlog::level::critical;
  }
  return spdlog::level::info;
}

// ============================================================================
// Logger implementation
// ============================================================================

Logger::Logger() : logger_(nullptr) {}

Logger::~Logger() {
  if (logger_) {
    logger_->flush();
  }
}

core::Status Logger::Initialize(const LogConfig& config) {
  static std::mutex init_mutex;
  std::lock_guard<std::mutex> lock(init_mutex);

  if (instance_ != nullptr) {
    return core::AlreadyExistsError("Logger already initialized");
  }

  if (!config.IsValid()) {
    return core::InvalidArgumentError("Invalid logger configuration");
  }

  instance_ = std::unique_ptr<Logger>(new Logger());

  try {
    std::vector<spdlog::sink_ptr> sinks;

    // Console sink (if enabled)
    if (config.console_enabled) {
      if (config.console_colored) {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(ToSpdlogLevel(config.level));
        console_sink->set_pattern(config.pattern);
        sinks.push_back(console_sink);
      } else {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(ToSpdlogLevel(config.level));
        console_sink->set_pattern(config.pattern);
        sinks.push_back(console_sink);
      }
    }

    // File sink with rotation (if enabled)
    if (config.file_enabled) {
      auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
          config.file_path, config.max_file_size, config.max_files);
      file_sink->set_level(ToSpdlogLevel(config.level));
      file_sink->set_pattern(config.pattern);
      sinks.push_back(file_sink);
    }

    // Create logger (async or sync)
    if (config.async) {
      // Initialize thread pool for async logging
      spdlog::init_thread_pool(config.async_queue_size, 1);
      instance_->logger_ = std::make_shared<spdlog::async_logger>(
          "gvdb",
          sinks.begin(),
          sinks.end(),
          spdlog::thread_pool(),
          spdlog::async_overflow_policy::block);
    } else {
      instance_->logger_ = std::make_shared<spdlog::logger>(
          "gvdb", sinks.begin(), sinks.end());
    }

    instance_->logger_->set_level(ToSpdlogLevel(config.level));
    instance_->logger_->flush_on(spdlog::level::err);

    // Register as default logger
    spdlog::set_default_logger(instance_->logger_);

  } catch (const spdlog::spdlog_ex& ex) {
    instance_.reset();
    return core::InternalError(
        std::string("Failed to initialize logger: ") + ex.what());
  }

  return core::OkStatus();
}

Logger& Logger::Instance() {
  if (instance_ == nullptr) {
    // Auto-initialize with default config if not explicitly initialized
    LogConfig default_config;
    default_config.console_enabled = true;
    default_config.file_enabled = false;
    default_config.level = LogLevel::INFO;
    auto status = Initialize(default_config);
    (void)status;  // Ignore status for default initialization
  }
  return *instance_;
}

void Logger::SetLevel(LogLevel level) {
  if (logger_) {
    logger_->set_level(ToSpdlogLevel(level));
  }
}

void Logger::Flush() {
  if (logger_) {
    logger_->flush();
  }
}

void Logger::Shutdown() {
  if (instance_) {
    instance_->Flush();
    instance_.reset();
  }
  spdlog::shutdown();
}

// ============================================================================
// Template method implementations (header-only)
// ============================================================================
// Note: Template methods are implemented inline in the header file
// to avoid explicit instantiation issues

}  // namespace utils
}  // namespace gvdb
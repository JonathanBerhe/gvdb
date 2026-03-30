// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#ifndef GVDB_UTILS_LOGGER_H_
#define GVDB_UTILS_LOGGER_H_

#include <memory>
#include <string>

#include "core/status.h"

// Include spdlog for template implementations
#include "spdlog/spdlog.h"

namespace gvdb {
namespace utils {

// ============================================================================
// LogLevel - Log severity levels
// ============================================================================
enum class LogLevel {
  TRACE,     // Very detailed debug information
  DEBUG,     // Debug information
  INFO,      // Informational messages
  WARN,      // Warning messages
  ERROR,     // Error messages
  CRITICAL   // Critical/fatal errors
};

// ============================================================================
// LogConfig - Logger configuration
// ============================================================================
struct LogConfig {
  // Log level
  LogLevel level = LogLevel::INFO;

  // Console logging
  bool console_enabled = true;
  bool console_colored = true;

  // File logging
  bool file_enabled = true;
  std::string file_path = "./logs/gvdb.log";
  size_t max_file_size = 100 * 1024 * 1024;  // 100 MB
  size_t max_files = 10;

  // Async logging
  bool async = true;
  size_t async_queue_size = 8192;

  // Log pattern
  std::string pattern = "[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] %v";

  [[nodiscard]] bool IsValid() const {
    return max_file_size > 0 && max_files > 0;
  }
};

// ============================================================================
// Logger - Structured logging wrapper around spdlog
// ============================================================================
class Logger {
 public:
  // Initialize global logger
  static core::Status Initialize(const LogConfig& config);

  // Get global logger instance
  static Logger& Instance();

  // Disable copy and move
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(Logger&&) = delete;

  // Set log level at runtime
  void SetLevel(LogLevel level);

  // Logging methods
  template <typename... Args>
  void Trace(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->trace(fmt, std::forward<Args>(args)...);
    }
  }

  template <typename... Args>
  void Debug(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->debug(fmt, std::forward<Args>(args)...);
    }
  }

  template <typename... Args>
  void Info(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->info(fmt, std::forward<Args>(args)...);
    }
  }

  template <typename... Args>
  void Warn(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->warn(fmt, std::forward<Args>(args)...);
    }
  }

  template <typename... Args>
  void Error(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->error(fmt, std::forward<Args>(args)...);
    }
  }

  template <typename... Args>
  void Critical(const std::string& fmt, Args&&... args) {
    if (logger_) {
      logger_->critical(fmt, std::forward<Args>(args)...);
    }
  }

  // Flush logs (useful before shutdown)
  void Flush();

  // Shutdown logger
  static void Shutdown();

  // Destructor needs to be public for std::unique_ptr
  ~Logger();

 private:
  Logger();

  std::shared_ptr<spdlog::logger> logger_;
  static std::unique_ptr<Logger> instance_;
};

// ============================================================================
// Convenience macros for logging
// ============================================================================
#define LOG_TRACE(...) ::gvdb::utils::Logger::Instance().Trace(__VA_ARGS__)
#define LOG_DEBUG(...) ::gvdb::utils::Logger::Instance().Debug(__VA_ARGS__)
#define LOG_INFO(...) ::gvdb::utils::Logger::Instance().Info(__VA_ARGS__)
#define LOG_WARN(...) ::gvdb::utils::Logger::Instance().Warn(__VA_ARGS__)
#define LOG_ERROR(...) ::gvdb::utils::Logger::Instance().Error(__VA_ARGS__)
#define LOG_CRITICAL(...) ::gvdb::utils::Logger::Instance().Critical(__VA_ARGS__)

}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_LOGGER_H_
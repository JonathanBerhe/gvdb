#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

#include "utils/logger.h"
#include "utils/thread_pool.h"
#include "utils/timer.h"

namespace fs = std::filesystem;

namespace gvdb {
namespace utils {
namespace test {

// ============================================================================
// Logger Tests
// ============================================================================

class LoggerTest {
 public:
  LoggerTest() {
    // Clean up any existing logger
    Logger::Shutdown();

    // Create temp log directory
    log_dir_ = fs::temp_directory_path() / "gvdb_test_logs";
    fs::create_directories(log_dir_);
  }

  ~LoggerTest() {
    Logger::Shutdown();

    // Clean up log files
    if (fs::exists(log_dir_)) {
      fs::remove_all(log_dir_);
    }
  }

  fs::path log_dir_;
};

TEST_CASE_FIXTURE(LoggerTest, "InitializeWithDefaultConfig") {
  LogConfig config;
  config.console_enabled = true;
  config.file_enabled = false;
  config.level = LogLevel::INFO;

  auto status = Logger::Initialize(config);
  CHECK_MESSAGE(status.ok(), status.message());

  // Should be able to log without errors
  Logger::Instance().Info("Test message");
  Logger::Instance().Debug("Debug message");  // Should be filtered out (level is INFO)

  Logger::Instance().Flush();
}

TEST_CASE_FIXTURE(LoggerTest, "InitializeWithFileLogging") {
  LogConfig config;
  config.console_enabled = false;
  config.file_enabled = true;
  config.file_path = (log_dir_ / "test.log").string();
  config.level = LogLevel::DEBUG;

  auto status = Logger::Initialize(config);
  CHECK_MESSAGE(status.ok(), status.message());

  Logger::Instance().Info("Test info message");
  Logger::Instance().Debug("Test debug message");

  Logger::Instance().Flush();

  // Verify log file was created
  CHECK(fs::exists(config.file_path));
}

TEST_CASE_FIXTURE(LoggerTest, "DoubleInitializeFails") {
  LogConfig config;
  config.console_enabled = true;
  config.file_enabled = false;

  auto status1 = Logger::Initialize(config);
  CHECK(status1.ok());

  auto status2 = Logger::Initialize(config);
  CHECK_FALSE(status2.ok());
  CHECK_EQ(status2.code(), absl::StatusCode::kAlreadyExists);
}

TEST_CASE_FIXTURE(LoggerTest, "InvalidConfigFails") {
  LogConfig config;
  config.max_file_size = 0;  // Invalid

  auto status = Logger::Initialize(config);
  CHECK_FALSE(status.ok());
  CHECK_EQ(status.code(), absl::StatusCode::kInvalidArgument);
}

TEST_CASE_FIXTURE(LoggerTest, "SetLogLevel") {
  LogConfig config;
  config.console_enabled = false;
  config.file_enabled = true;
  config.file_path = (log_dir_ / "level_test.log").string();
  config.level = LogLevel::WARN;

  auto status = Logger::Initialize(config);
  REQUIRE(status.ok());

  Logger::Instance().Debug("Should not appear");
  Logger::Instance().Info("Should not appear");
  Logger::Instance().Warn("Should appear");
  Logger::Instance().Error("Should appear");

  Logger::Instance().Flush();

  // Ensure logger is fully shut down and flushed before reading file
  Logger::Shutdown();

  // Read log file and verify only WARN and ERROR messages are present
  std::ifstream log_file(config.file_path);
  std::string content((std::istreambuf_iterator<char>(log_file)),
                      std::istreambuf_iterator<char>());

  CHECK_EQ(content.find("Should not appear"), std::string::npos);
  CHECK_NE(content.find("Should appear"), std::string::npos);
}

TEST_CASE_FIXTURE(LoggerTest, "LogWithParameters") {
  LogConfig config;
  config.console_enabled = true;
  config.file_enabled = false;
  config.level = LogLevel::INFO;

  auto status = Logger::Initialize(config);
  REQUIRE(status.ok());

  Logger::Instance().Info("Test with int: {}", 42);
  Logger::Instance().Info("Test with string: {}", "hello");
  Logger::Instance().Info("Test with multiple: {} {} {}", 1, "two", 3.14);

  Logger::Instance().Flush();
}

// ============================================================================
// ThreadPool Tests
// ============================================================================

TEST_CASE("ThreadPoolTest - CreateWithDefaultThreads") {
  ThreadPool pool;
  CHECK_GT(pool.size(), 0);
}

TEST_CASE("ThreadPoolTest - CreateWithSpecificThreads") {
  ThreadPool pool(4);
  CHECK_EQ(pool.size(), 4);
}

TEST_CASE("ThreadPoolTest - EnqueueSimpleTask") {
  ThreadPool pool(2);

  std::atomic<int> counter{0};

  auto future = pool.enqueue([&counter] {
    counter.fetch_add(1);
    return 42;
  });

  CHECK_EQ(future.get(), 42);
  CHECK_EQ(counter.load(), 1);
}

TEST_CASE("ThreadPoolTest - EnqueueMultipleTasks") {
  ThreadPool pool(4);

  std::atomic<int> counter{0};
  std::vector<std::future<int>> futures;

  for (int i = 0; i < 100; ++i) {
    futures.push_back(pool.enqueue([&counter, i] {
      counter.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      return i;
    }));
  }

  // Wait for all tasks and verify results
  for (int i = 0; i < 100; ++i) {
    CHECK_EQ(futures[i].get(), i);
  }

  CHECK_EQ(counter.load(), 100);
}

TEST_CASE("ThreadPoolTest - EnqueueWithParameters") {
  ThreadPool pool(2);

  auto add = [](int a, int b) { return a + b; };

  auto future = pool.enqueue(add, 10, 32);
  CHECK_EQ(future.get(), 42);
}

TEST_CASE("ThreadPoolTest - ParallelExecution") {
  ThreadPool pool(4);

  std::atomic<int> concurrent_count{0};
  std::atomic<int> max_concurrent{0};

  std::vector<std::future<void>> futures;

  for (int i = 0; i < 8; ++i) {
    futures.push_back(pool.enqueue([&concurrent_count, &max_concurrent] {
      int current = concurrent_count.fetch_add(1) + 1;

      // Update max concurrent
      int expected = max_concurrent.load();
      while (expected < current &&
             !max_concurrent.compare_exchange_weak(expected, current)) {
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      concurrent_count.fetch_sub(1);
    }));
  }

  for (auto& future : futures) {
    future.get();
  }

  // With 4 threads and 8 tasks, should have seen 4 concurrent executions
  CHECK_GE(max_concurrent.load(), 4);
}

TEST_CASE("ThreadPoolTest - DestructorWaitsForTasks") {
  std::atomic<int> counter{0};

  {
    ThreadPool pool(2);

    for (int i = 0; i < 10; ++i) {
      pool.enqueue([&counter] {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        counter.fetch_add(1);
      });
    }

    // pool destructor should wait for all tasks
  }

  // All tasks should have completed
  CHECK_EQ(counter.load(), 10);
}

// ============================================================================
// Timer Tests
// ============================================================================

TEST_CASE("TimerTest - MeasureElapsedTime") {
  Timer timer;

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int64_t elapsed_millis = timer.elapsed_millis();
  CHECK_GE(elapsed_millis, 90);   // Allow some tolerance
  CHECK_LE(elapsed_millis, 150);
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST_CASE("UtilsIntegrationTest - ThreadPoolWithTimerAndLogger") {
  // Initialize logger
  LogConfig config;
  config.console_enabled = true;
  config.file_enabled = false;
  config.level = LogLevel::INFO;
  auto status = Logger::Initialize(config);
  REQUIRE(status.ok());

  ThreadPool pool(4);

  std::atomic<int> completed{0};
  std::vector<std::future<void>> futures;

  Timer overall_timer;

  for (int i = 0; i < 10; ++i) {
    futures.push_back(pool.enqueue([i, &completed] {
      Timer task_timer;

      // Simulate work
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      int64_t elapsed = task_timer.elapsed_micros();
      Logger::Instance().Info("Task {} completed in {} us", i, elapsed);

      completed.fetch_add(1);
    }));
  }

  // Wait for all tasks
  for (auto& future : futures) {
    future.get();
  }

  int64_t total_elapsed = overall_timer.elapsed_millis();
  Logger::Instance().Info("All {} tasks completed in {} ms", completed.load(), total_elapsed);

  CHECK_EQ(completed.load(), 10);

  Logger::Instance().Flush();
  Logger::Shutdown();
}

}  // namespace test
}  // namespace utils
}  // namespace gvdb

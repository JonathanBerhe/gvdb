// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include <doctest/doctest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "support/temp_dir.h"
#include "support/wait.h"
#include "config/config.h"
#include "common/file_util.h"
#include "common/logger.h"
#include "common/thread_pool.h"
#include "common/timer.h"

namespace fs = std::filesystem;

namespace gvdb {
namespace common {
namespace test {

// ============================================================================
// Logger Tests
// ============================================================================

class LoggerTest {
 public:
  LoggerTest() {
    // Clean up any existing logger
    Logger::Shutdown();
  }

  ~LoggerTest() { Logger::Shutdown(); }

  // Log files go to a folder that no other test or test process uses. It is
  // removed after the destructor has shut the logger down.
  gvdb::test::ScopedTempDir log_dir_{"gvdb_test_logs"};
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

      // Stay in the task until four tasks have run at the same time, so the
      // result does not depend on how the OS schedules the threads. If the
      // pool cannot run four tasks at once, the wait times out and the check
      // below fails.
      gvdb::test::WaitFor([&max_concurrent] { return max_concurrent.load() >= 4; });
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
  // A fake clock: time moves only when the test moves it, so the elapsed
  // time is exact.
  auto now = std::chrono::steady_clock::time_point{} + std::chrono::hours(1);
  Timer timer([&now] { return now; });

  now += std::chrono::milliseconds(100);

  int64_t elapsed_millis = timer.elapsed_millis();
  CHECK_EQ(elapsed_millis, 100);
  CHECK_EQ(timer.elapsed_micros(), 100000);
}

// ============================================================================
// Config Tests
// ============================================================================

// Loads YAML through Config::load_from_file from a file in a unique
// temporary folder, which is removed afterwards.
class ConfigFileTest {
 public:
  absl::StatusOr<config::GVDBConfig> Load(const std::string& yaml) {
    const auto path = dir_ / "config.yaml";
    {
      std::ofstream out(path);
      REQUIRE(out.is_open());
      out << yaml;
    }
    return config::Config::load_from_file(path.string());
  }

  gvdb::test::ScopedTempDir dir_{"gvdb_config_test"};
};

TEST_CASE_FIXTURE(ConfigFileTest,
                  "ConfigTest - LoadsAuthRolesObjectStoreAndLocalBackupDir") {
  auto config = Load(R"(
server:
  auth:
    enabled: true
    api_keys:
      - legacy-key-1
      - legacy-key-2
    roles:
      - key: admin-key
        role: admin
      - key: rw-key
        role: readwrite
        collections: [products, embeddings]
      - key: ro-key
        role: readonly
        collections: ["*"]
      - key: ca-key
        role: collection_admin
        collections: [products]
storage:
  local_backup_dir: /var/lib/gvdb/backups
  object_store:
    type: minio
    endpoint: localhost:9000
    access_key: test-access-key
    secret_key: test-secret-key
    bucket: gvdb-test
    region: eu-west-1
    prefix: cluster-a
    project: test-project
    credentials_path: /etc/gvdb/gcs.json
    use_ssl: false
    local_cache_size_mb: 512
    upload_threads: 4
)");
  INFO(config.status().message());
  REQUIRE(config.ok());

  const auto& auth = config->server.auth;
  CHECK(auth.enabled);
  const std::vector<std::string> legacy_keys = {"legacy-key-1", "legacy-key-2"};
  CHECK_EQ(auth.api_keys, legacy_keys);

  REQUIRE_EQ(auth.roles.size(), 4u);
  CHECK_EQ(auth.roles[0].key, "admin-key");
  CHECK_EQ(auth.roles[0].role, "admin");
  CHECK(auth.roles[0].collections.empty());

  const std::vector<std::string> rw_collections = {"products", "embeddings"};
  CHECK_EQ(auth.roles[1].key, "rw-key");
  CHECK_EQ(auth.roles[1].role, "readwrite");
  CHECK_EQ(auth.roles[1].collections, rw_collections);

  const std::vector<std::string> all_collections = {"*"};
  CHECK_EQ(auth.roles[2].key, "ro-key");
  CHECK_EQ(auth.roles[2].role, "readonly");
  CHECK_EQ(auth.roles[2].collections, all_collections);

  const std::vector<std::string> ca_collections = {"products"};
  CHECK_EQ(auth.roles[3].key, "ca-key");
  CHECK_EQ(auth.roles[3].role, "collection_admin");
  CHECK_EQ(auth.roles[3].collections, ca_collections);

  const auto& storage = config->storage;
  CHECK_EQ(storage.local_backup_dir, "/var/lib/gvdb/backups");
  CHECK_EQ(storage.object_store.type, "minio");
  CHECK_EQ(storage.object_store.endpoint, "localhost:9000");
  CHECK_EQ(storage.object_store.access_key, "test-access-key");
  CHECK_EQ(storage.object_store.secret_key, "test-secret-key");
  CHECK_EQ(storage.object_store.bucket, "gvdb-test");
  CHECK_EQ(storage.object_store.region, "eu-west-1");
  CHECK_EQ(storage.object_store.prefix, "cluster-a");
  CHECK_EQ(storage.object_store.project, "test-project");
  CHECK_EQ(storage.object_store.credentials_path, "/etc/gvdb/gcs.json");
  CHECK_FALSE(storage.object_store.use_ssl);
  CHECK_EQ(storage.object_store_cache_size_mb, 512);
  CHECK_EQ(storage.object_store_upload_threads, 4);
}

TEST_CASE_FIXTURE(ConfigFileTest, "ConfigTest - ObjectStoreKeysLeftOutGetDefaults") {
  auto config = Load(R"(
storage:
  object_store:
    type: s3
    endpoint: s3.example.com
    bucket: gvdb-data
)");
  INFO(config.status().message());
  REQUIRE(config.ok());

  const auto& storage = config->storage;
  CHECK_EQ(storage.object_store.type, "s3");
  CHECK_EQ(storage.object_store.endpoint, "s3.example.com");
  CHECK_EQ(storage.object_store.bucket, "gvdb-data");
  CHECK(storage.object_store.access_key.empty());
  CHECK(storage.object_store.secret_key.empty());
  CHECK(storage.object_store.project.empty());
  CHECK(storage.object_store.credentials_path.empty());
  CHECK_EQ(storage.object_store.region, "us-east-1");
  CHECK_EQ(storage.object_store.prefix, "gvdb");
  CHECK(storage.object_store.use_ssl);
  CHECK_EQ(storage.object_store_cache_size_mb, 256);
  CHECK_EQ(storage.object_store_upload_threads, 2);

  // Without local_backup_dir, local backup targets stay disabled.
  CHECK(storage.local_backup_dir.empty());

  const auto& auth = config->server.auth;
  CHECK_FALSE(auth.enabled);
  CHECK(auth.api_keys.empty());
  CHECK(auth.roles.empty());
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

TEST_CASE("ReadFileToString returns the whole file, and NotFound for a missing file") {
  gvdb::test::ScopedTempDir dir("gvdb_file_util");
  const std::string path = (dir / "data.bin").string();
  const std::string contents("line 1\nline 2\0binary", 20);
  std::ofstream(path, std::ios::binary) << contents;

  auto read = ReadFileToString(path);
  REQUIRE(read.ok());
  CHECK_EQ(*read, contents);
  CHECK(absl::IsNotFound(ReadFileToString((dir / "missing").string()).status()));
}

TEST_CASE("WriteFileAtomically replaces the file and leaves no temp file") {
  gvdb::test::ScopedTempDir dir("gvdb_file_util");
  const std::string path = (dir / "registry.json").string();

  REQUIRE(WriteFileAtomically(path, "first").ok());
  REQUIRE(WriteFileAtomically(path, "second").ok());
  CHECK_EQ(*ReadFileToString(path), "second");
  CHECK_FALSE(fs::exists(path + ".tmp"));
}

TEST_CASE("WriteFileAtomically fails when the folder does not exist") {
  gvdb::test::ScopedTempDir dir("gvdb_file_util");
  const std::string path = (dir / "no_such_folder" / "registry.json").string();

  auto status = WriteFileAtomically(path, "data");
  CHECK_FALSE(status.ok());
  CHECK_FALSE(fs::exists(path));
}

}  // namespace test
}  // namespace common
}  // namespace gvdb

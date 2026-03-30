// Copyright 2026 jonathanberhe
// Licensed under the Apache License, Version 2.0

#include "utils/thread_pool.h"

namespace gvdb {
namespace utils {

ThreadPool::ThreadPool(size_t threads) : stop_(false) {
  // Use hardware concurrency if threads == 0
  size_t num_threads = threads;
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;  // Fallback if detection fails
    }
  }

  workers_.reserve(num_threads);

  // Create worker threads
  for (size_t i = 0; i < num_threads; ++i) {
    workers_.emplace_back([this] {
      while (true) {
        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->queue_mutex_);

          // Wait for a task or stop signal
          this->condition_.wait(lock, [this] {
            return this->stop_ || !this->tasks_.empty();
          });

          // Exit if stopped and no more tasks
          if (this->stop_ && this->tasks_.empty()) {
            return;
          }

          // Get next task
          task = std::move(this->tasks_.front());
          this->tasks_.pop();
        }

        // Execute task (outside the lock)
        task();
      }
    });
  }
}

ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    stop_ = true;
  }

  // Wake up all threads
  condition_.notify_all();

  // Wait for all threads to finish
  for (std::thread& worker : workers_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

size_t ThreadPool::pending() const {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  return tasks_.size();
}

}  // namespace utils
}  // namespace gvdb
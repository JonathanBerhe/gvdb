#ifndef GVDB_UTILS_THREAD_POOL_H_
#define GVDB_UTILS_THREAD_POOL_H_

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace gvdb {
namespace utils {

// ============================================================================
// ThreadPool - Fixed-size thread pool for parallel task execution
// ============================================================================
class ThreadPool {
 public:
  // Create thread pool with specified number of worker threads
  // If threads == 0, uses hardware_concurrency()
  explicit ThreadPool(size_t threads = 0);

  // Disable copy and move
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  // Destructor waits for all tasks to complete
  ~ThreadPool();

  // Enqueue a task for execution
  // Returns a future that will contain the result
  template <typename F, typename... Args>
  auto enqueue(F&& f, Args&&... args)
      -> std::future<typename std::invoke_result<F, Args...>::type>;

  // Get number of worker threads
  [[nodiscard]] size_t size() const noexcept { return workers_.size(); }

  // Get number of pending tasks
  [[nodiscard]] size_t pending() const;

 private:
  // Worker threads
  std::vector<std::thread> workers_;

  // Task queue
  std::queue<std::function<void()>> tasks_;

  // Synchronization
  mutable std::mutex queue_mutex_;
  std::condition_variable condition_;
  bool stop_ = false;
};

// ============================================================================
// Template implementation
// ============================================================================

template <typename F, typename... Args>
auto ThreadPool::enqueue(F&& f, Args&&... args)
    -> std::future<typename std::invoke_result<F, Args...>::type> {
  using return_type = typename std::invoke_result<F, Args...>::type;

  // Create packaged task
  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> result = task->get_future();

  {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    // Don't allow enqueueing after stopping the pool
    if (stop_) {
      throw std::runtime_error("Cannot enqueue on stopped ThreadPool");
    }

    tasks_.emplace([task]() { (*task)(); });
  }

  condition_.notify_one();
  return result;
}

}  // namespace utils
}  // namespace gvdb

#endif  // GVDB_UTILS_THREAD_POOL_H_

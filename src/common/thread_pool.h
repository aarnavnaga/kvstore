#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace kv {

// ThreadPool runs submitted tasks on a fixed set of threads, in FIFO order.
class ThreadPool {
 public:
  explicit ThreadPool(size_t threads) {
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] { Run(); });
    }
  }

  ~ThreadPool() {
    {
      std::lock_guard lk(mu_);
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      t.join();
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  void Submit(std::function<void()> task) {
    {
      std::lock_guard lk(mu_);
      tasks_.push_back(std::move(task));
    }
    cv_.notify_one();
  }

 private:
  void Run() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock lk(mu_);
        cv_.wait(lk, [&] { return stopping_ || !tasks_.empty(); });
        if (tasks_.empty()) {
          return;  // stopping, and every queued task has run
        }
        task = std::move(tasks_.front());
        tasks_.pop_front();
      }
      task();
    }
  }

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> tasks_;
  bool stopping_ = false;
  std::vector<std::thread> workers_;
};

}  // namespace kv

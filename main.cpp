#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <syncstream>
#include <thread>
#include <vector>

struct task_t
{
  void operator()();
};

void task_t::operator()()
{
  using namespace std::chrono_literals;
  std::this_thread::sleep_for(200ms);
  std::osyncstream(std::cout) << "!\n";
}

struct th_pool_t
{
  using f_t = std::function< void(void) >;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();
  void submit(f_t f);

  void shutdown_now();
  void wait();
  void pause();
  void resume();

private:
  void execute();

  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable done_cv_;
  std::condition_variable resume_cv_;

  bool stop_{false};
  bool shutdown_now_{false};
  bool paused_{false};
  size_t active_tasks_{0};

  std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();

  if (stop_) {
    mx_.unlock();
    throw std::runtime_error("pool stopped");
  }

  try {
    tasks_.push(std::move(f));
  } catch (...) {
    mx_.unlock();
    throw;
  }

  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() {
    execute();
  };

  try {
    for (; k; --k) {
      ths_.emplace_back(thf);
    }
  } catch (...) {
    mx_.lock();
    stop_ = true;
    shutdown_now_ = true;
    mx_.unlock();
    cd_.notify_all();
    resume_cv_.notify_all();

    for (auto &th : ths_) {
      if (th.joinable()) {
        th.join();
      }
    }

    throw;
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;
  mx_.unlock();

  cd_.notify_all();
  resume_cv_.notify_all();

  for (auto &th : ths_) {
    if (th.joinable()) {
      th.join();
    }
  }
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);

  auto has_work = [this] {
    return stop_ || !tasks_.empty();
  };
  auto not_paused = [this] {
    return !paused_ || shutdown_now_;
  };

  while (true) {
    cd_.wait(lck, has_work);

    if (stop_ && tasks_.empty()) {
      break;
    }

    auto task = std::move(tasks_.front());
    tasks_.pop();
    ++active_tasks_;

    resume_cv_.wait(lck, not_paused);
    if (shutdown_now_) {
      --active_tasks_;
      break;
    }

    lck.unlock();

    try {
      task();
    } catch (...) {
    }

    lck.lock();
    --active_tasks_;
    if (active_tasks_ == 0 && tasks_.empty()) {
      done_cv_.notify_all();
    }
  }
}

void th_pool_t::wait()
{
  std::unique_lock< std::mutex > lck(mx_);
  done_cv_.wait(lck, [this] {
    return tasks_.empty() && active_tasks_ == 0;
  });
}

void th_pool_t::pause()
{
  mx_.lock();
  paused_ = true;
  mx_.unlock();
}

void th_pool_t::resume()
{
  mx_.lock();
  paused_ = false;
  mx_.unlock();
  resume_cv_.notify_all();
}

void th_pool_t::shutdown_now()
{
  mx_.lock();
  stop_ = true;
  shutdown_now_ = true;
  while (!tasks_.empty()) {
    tasks_.pop();
  }
  mx_.unlock();

  cd_.notify_all();
  done_cv_.notify_all();
  resume_cv_.notify_all();

  for (auto &th : ths_) {
    if (th.joinable()) {
      th.join();
    }
  }
}

int main()
{
  th_pool_t pool(5);

  for (size_t i = 0; i < 10; ++i) {
    try {
      pool.submit(task_t{});
    } catch (const std::runtime_error &e) {
      std::osyncstream(std::cout) << "submitting while stopped\n";
      break;
    } catch (...) {
      std::osyncstream(std::cout) << "unknown error while submitting\n";
      break;
    }
  }

  pool.wait();
  std::osyncstream(std::cout) << "all tasks completed\n";

  pool.pause();
  std::osyncstream(std::cout) << "pool paused\n";

  for (size_t i = 0; i < 5; ++i) {
    try {
      pool.submit(task_t{});
    } catch (const std::runtime_error &e) {
      std::osyncstream(std::cout) << "submitting while stopped\n";
      break;
    } catch (...) {
      std::osyncstream(std::cout) << "unknown error while submitting\n";
      break;
    }
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  pool.resume();
  std::osyncstream(std::cout) << "pool resumed\n";

  pool.wait();
  std::osyncstream(std::cout) << "all tasks completed\n";

  for (size_t i = 0; i < 10; ++i) {
    try {
      pool.submit(task_t{});
    } catch (const std::runtime_error &e) {
      std::osyncstream(std::cout) << "submitting while stopped\n";
      break;
    } catch (...) {
      std::osyncstream(std::cout) << "unknown error while submitting\n";
      break;
    }
  }

  pool.shutdown_now();
  std::osyncstream(std::cout) << "pool shutdown\n";
}
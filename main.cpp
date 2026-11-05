#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>
#include <syncstream>

struct task_t
{
  void operator()();
};

void task_t::operator()() {
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

private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  bool stop_{false};
  std::vector< std::thread > ths_;
};

void th_pool_t::submit(f_t f)
{
  mx_.lock();
  tasks_.push(std::move(f));
  mx_.unlock();
  cd_.notify_one();
}

th_pool_t::th_pool_t(unsigned int k)
{
  auto thf = [this]() {
    execute();
  };

  for (; k; --k) {
    ths_.emplace_back(thf);
  }
}

th_pool_t::~th_pool_t()
{
  mx_.lock();
  stop_ = true;

  mx_.unlock();
  cd_.notify_all();

  for (auto &&th : ths_) {
    th.join();
  }
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);
  do {
    auto proceed = [&]() {
      return !tasks_.empty() || stop_;
    };

    cd_.wait(lck, proceed);

    if (tasks_.empty()){
      continue;
    }

    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();

    try {
      task();
    } catch (...) {}

    lck.lock();
  } while (!stop_ || !tasks_.empty());
}

int main()
{
  th_pool_t pool(5);
  for (size_t i = 0; i < 10; ++i){
    pool.submit(task_t{});
  }
}
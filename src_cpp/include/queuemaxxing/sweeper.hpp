#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "queuemaxxing/engine.hpp"

namespace queuemaxxing {

class VisibilitySweeper {
 public:
  explicit VisibilitySweeper(std::chrono::milliseconds interval = std::chrono::milliseconds(100));
  ~VisibilitySweeper();

  void add(QueueEngine* engine);
  void start();
  void stop();

 private:
  std::chrono::milliseconds interval_;
  std::vector<QueueEngine*> engines_;
  std::atomic<bool> stop_{true};
  std::thread thread_;
};

}  // namespace queuemaxxing

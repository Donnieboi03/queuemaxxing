#include "queuemaxxing/sweeper.hpp"

#include <iostream>

namespace queuemaxxing {

VisibilitySweeper::VisibilitySweeper(std::chrono::milliseconds interval)
    : interval_(interval) {}

VisibilitySweeper::~VisibilitySweeper() { stop(); }

void VisibilitySweeper::add(QueueEngine* engine) {
  engines_.push_back(engine);
}

void VisibilitySweeper::start() {
  if (thread_.joinable()) return;
  stop_ = false;
  thread_ = std::thread([this] {
    while (!stop_) {
      for (auto* e : engines_) {
        try {
          if (e) e->tick();
        } catch (...) {
        }
      }
      std::this_thread::sleep_for(interval_);
    }
  });
}

void VisibilitySweeper::stop() {
  stop_ = true;
  if (thread_.joinable()) thread_.join();
}

}  // namespace queuemaxxing

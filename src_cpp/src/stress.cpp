#include "queuemaxxing/engine.hpp"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

using namespace queuemaxxing;
using clock_ = std::chrono::steady_clock;

int main(int argc, char** argv) {
  int messages = 10000;
  int producers = 4;
  int consumers = 4;
  bool wal = false;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--messages" && i + 1 < argc) messages = std::atoi(argv[++i]);
    else if (a == "--producers" && i + 1 < argc) producers = std::atoi(argv[++i]);
    else if (a == "--consumers" && i + 1 < argc) consumers = std::atoi(argv[++i]);
    else if (a == "--wal") wal = true;
  }

  std::optional<std::filesystem::path> dir;
  if (wal) {
    dir = std::filesystem::path("tmp/data") /
          ("stress-cpp-" + std::to_string(std::time(nullptr)));
    std::filesystem::create_directories(*dir);
  }

  QueueEngine eng(QueueConfig{"stress", OrderMode::Fifo, 0, 60}, dir, nullptr, wal);

  int per = messages / producers;
  int rem = messages % producers;
  auto t0 = clock_::now();
  std::vector<std::thread> prod;
  for (int k = 0; k < producers; ++k) {
    int count = per + (k == producers - 1 ? rem : 0);
    prod.emplace_back([&eng, k, count] {
      for (int i = 0; i < count; ++i) eng.enqueue("p" + std::to_string(k) + "-" + std::to_string(i), i % 16);
    });
  }
  for (auto& t : prod) t.join();
  auto t1 = clock_::now();

  std::mutex mu;
  std::vector<std::string> got;
  got.reserve(messages);
  std::vector<std::thread> cons;
  for (int c = 0; c < consumers; ++c) {
    cons.emplace_back([&] {
      while (true) {
        {
          std::lock_guard lock(mu);
          if (static_cast<int>(got.size()) >= messages) return;
        }
        auto m = eng.receive();
        if (!m) {
          std::this_thread::sleep_for(std::chrono::microseconds(200));
          continue;
        }
        eng.ack(*m->transit_id);
        std::lock_guard lock(mu);
        got.push_back(m->id);
      }
    });
  }
  for (auto& t : cons) t.join();
  auto t2 = clock_::now();

  double enq_s = std::chrono::duration<double>(t1 - t0).count();
  double out_s = std::chrono::duration<double>(t2 - t1).count();
  nlohmann::json report{{"mode", "engine-cpp"},
                        {"wal", wal},
                        {"messages", messages},
                        {"acked", got.size()},
                        {"enqueue_msg_per_s", messages / enq_s},
                        {"consume_msg_per_s", got.size() / out_s}};
  std::filesystem::create_directories("tmp");
  auto path = std::filesystem::path("tmp") /
              ("stress-cpp-" + std::to_string(std::time(nullptr)) + ".json");
  std::ofstream(path) << report.dump(2) << "\n";

  std::cout << "--- stress summary ---\n"
            << "mode: engine-cpp wal=" << wal << "\n"
            << "enqueue msg/s: " << report["enqueue_msg_per_s"] << "\n"
            << "consume msg/s: " << report["consume_msg_per_s"] << "\n"
            << "report: " << path << "\n";
  return got.size() == static_cast<size_t>(messages) ? 0 : 1;
}

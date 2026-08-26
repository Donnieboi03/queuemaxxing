#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <nlohmann/json.hpp>

#include "queuemaxxing/models.hpp"
#include "queuemaxxing/wal.hpp"

namespace queuemaxxing {

class QueueEngine {
 public:
  using Clock = std::function<double()>;

  QueueEngine(QueueConfig config, std::optional<std::filesystem::path> data_dir = std::nullopt,
              Clock clock = nullptr, bool durable = true, bool persist_meta = true);

  static QueueEngine open(const std::filesystem::path& data_dir, Clock clock = nullptr);

  Message enqueue(const std::string& body, int priority = 0,
                  std::optional<double> delay = std::nullopt,
                  std::optional<std::string> message_id = std::nullopt);
  std::optional<Message> receive();
  bool ack(const std::string& transit_id);
  std::vector<std::string> tick();

  const QueueConfig& config() const { return config_; }
  std::unordered_map<std::string, int> depths() const;

 private:
  struct StagedItem {
    double available_at;
    std::int64_t order_seq;
    std::string id;
    bool operator>(const StagedItem& o) const {
      if (available_at != o.available_at) return available_at > o.available_at;
      if (order_seq != o.order_seq) return order_seq > o.order_seq;
      return id > o.id;
    }
  };
  struct ReadyItem {
    int neg_priority;
    std::int64_t seq_key;
    std::string id;
    bool operator>(const ReadyItem& o) const {
      if (neg_priority != o.neg_priority) return neg_priority > o.neg_priority;
      if (seq_key != o.seq_key) return seq_key > o.seq_key;
      return id > o.id;
    }
  };

  void append(const nlohmann::json& event);
  void replay();
  void place_new(Message& msg, double now);
  void push_staged(Message& msg);
  void push_ready(Message& msg);
  ReadyItem ready_key(const Message& msg) const;
  std::optional<Message*> pop_staged_due(double now);
  std::optional<Message*> pop_ready();
  std::vector<std::string> expire_and_promote(double now);

  QueueConfig config_;
  Clock clock_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, Message> store_;
  std::priority_queue<StagedItem, std::vector<StagedItem>, std::greater<>> staged_;
  std::priority_queue<ReadyItem, std::vector<ReadyItem>, std::greater<>> ready_;
  std::unordered_map<std::string, std::string> inflight_;
  std::unordered_set<std::string> acked_;
  std::int64_t order_seq_{0};
  std::unique_ptr<Wal> owned_wal_;
  Wal* wal_{nullptr};
};

}  // namespace queuemaxxing

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace queuemaxxing {

enum class OrderMode { Fifo, Lifo };

enum class MessageState { Staged, Ready, InFlight };

struct QueueConfig {
  std::string name;
  OrderMode order{OrderMode::Fifo};
  double default_delay{0.0};
  double visibility_timeout{30.0};
};

struct Message {
  std::string id;
  std::string body;
  int priority{0};
  std::int64_t order_seq{0};
  double available_at{0.0};
  MessageState state{MessageState::Ready};
  std::optional<std::string> transit_id;
  std::optional<double> visible_again_at;
  int delivery_count{0};
};

}  // namespace queuemaxxing

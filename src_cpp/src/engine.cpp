#include "queuemaxxing/engine.hpp"

#include <chrono>
#include <cstdio>
#include <memory>
#include <random>
#include <stdexcept>

namespace queuemaxxing {
namespace {

double default_clock() {
  using clock = std::chrono::system_clock;
  return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
}

std::string make_id() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  std::uniform_int_distribution<std::uint64_t> dist;
  char buf[33];
  std::snprintf(buf, sizeof(buf), "%016llx%016llx",
                static_cast<unsigned long long>(dist(rng)),
                static_cast<unsigned long long>(dist(rng)));
  return std::string(buf);
}

}  // namespace

QueueEngine::QueueEngine(QueueConfig config, std::optional<std::filesystem::path> data_dir,
                         Clock clock, bool durable, bool persist_meta)
    : config_(std::move(config)), clock_(clock ? std::move(clock) : Clock{default_clock}) {
  if (durable && data_dir.has_value()) {
    owned_wal_ = std::make_unique<Wal>(*data_dir / "queue.wal");
    wal_ = owned_wal_.get();
    if (std::filesystem::exists(wal_->path()) && wal_->seq() > 0) {
      replay();
    } else if (persist_meta) {
      append({{"type", "queue_meta"},
              {"name", config_.name},
              {"order", config_.order == OrderMode::Fifo ? "fifo" : "lifo"},
              {"default_delay", config_.default_delay},
              {"visibility_timeout", config_.visibility_timeout}});
    }
  }
}

QueueEngine QueueEngine::open(const std::filesystem::path& data_dir, Clock clock) {
  Wal probe(data_dir / "queue.wal");
  QueueConfig cfg;
  bool found = false;
  for (const auto& e : probe.load()) {
    if (e.value("type", "") == "queue_meta") {
      cfg.name = e.at("name").get<std::string>();
      cfg.order = e.value("order", "fifo") == "lifo" ? OrderMode::Lifo : OrderMode::Fifo;
      cfg.default_delay = e.value("default_delay", 0.0);
      cfg.visibility_timeout = e.value("visibility_timeout", 30.0);
      found = true;
      break;
    }
  }
  if (!found) throw std::runtime_error("no queue_meta in wal");
  return QueueEngine(cfg, data_dir, std::move(clock), true, false);
}

void QueueEngine::append(const nlohmann::json& event) {
  if (wal_) wal_->append(event);
}

QueueEngine::ReadyItem QueueEngine::ready_key(const Message& msg) const {
  ReadyItem k;
  k.neg_priority = -msg.priority;
  k.seq_key = (config_.order == OrderMode::Fifo) ? msg.order_seq : -msg.order_seq;
  k.id = msg.id;
  return k;
}

void QueueEngine::push_staged(Message& msg) {
  msg.state = MessageState::Staged;
  staged_.push(StagedItem{msg.available_at, msg.order_seq, msg.id});
}

void QueueEngine::push_ready(Message& msg) {
  msg.state = MessageState::Ready;
  ready_.push(ready_key(msg));
}

void QueueEngine::place_new(Message& msg, double now) {
  if (msg.available_at <= now) push_ready(msg);
  else push_staged(msg);
}

std::optional<Message*> QueueEngine::pop_staged_due(double now) {
  while (!staged_.empty()) {
    auto top = staged_.top();
    if (top.available_at > now) return std::nullopt;
    staged_.pop();
    auto it = store_.find(top.id);
    if (it == store_.end() || it->second.state != MessageState::Staged) continue;
    return &it->second;
  }
  return std::nullopt;
}

std::optional<Message*> QueueEngine::pop_ready() {
  while (!ready_.empty()) {
    auto top = ready_.top();
    ready_.pop();
    auto it = store_.find(top.id);
    if (it == store_.end() || it->second.state != MessageState::Ready) continue;
    return &it->second;
  }
  return std::nullopt;
}

std::vector<std::string> QueueEngine::expire_and_promote(double now) {
  std::vector<std::string> redelivered;
  std::vector<std::pair<std::string, std::string>> expired;
  for (const auto& [tid, mid] : inflight_) {
    auto it = store_.find(mid);
    if (it == store_.end()) continue;
    if (it->second.visible_again_at && *it->second.visible_again_at <= now) {
      expired.emplace_back(tid, mid);
    }
  }
  for (const auto& [tid, mid] : expired) {
    append({{"type", "expire"}, {"id", mid}, {"transit_id", tid}});
    inflight_.erase(tid);
    auto& msg = store_.at(mid);
    msg.transit_id.reset();
    msg.visible_again_at.reset();
    push_ready(msg);
    redelivered.push_back(mid);
  }
  while (auto m = pop_staged_due(now)) {
    push_ready(**m);
  }
  return redelivered;
}

void QueueEngine::replay() {
  double now = clock_();
  for (const auto& e : wal_->load()) {
    const auto type = e.value("type", "");
    if (type == "queue_meta") {
      config_.name = e.at("name").get<std::string>();
      config_.order = e.value("order", "fifo") == "lifo" ? OrderMode::Lifo : OrderMode::Fifo;
      config_.default_delay = e.value("default_delay", 0.0);
      config_.visibility_timeout = e.value("visibility_timeout", 30.0);
    } else if (type == "enqueue") {
      Message msg;
      msg.id = e.at("id").get<std::string>();
      msg.body = e.at("body").get<std::string>();
      msg.priority = e.value("priority", 0);
      msg.order_seq = e.at("order_seq").get<std::int64_t>();
      msg.available_at = e.at("available_at").get<double>();
      store_[msg.id] = msg;
      order_seq_ = std::max(order_seq_, msg.order_seq);
      place_new(store_[msg.id], now);
    } else if (type == "receive") {
      auto id = e.at("id").get<std::string>();
      if (!store_.count(id) || acked_.count(id)) continue;
      auto& msg = store_[id];
      msg.state = MessageState::InFlight;
      msg.transit_id = e.at("transit_id").get<std::string>();
      msg.visible_again_at = e.at("visible_again_at").get<double>();
      msg.delivery_count += 1;
      inflight_[*msg.transit_id] = id;
    } else if (type == "ack") {
      auto tid = e.at("transit_id").get<std::string>();
      auto it = inflight_.find(tid);
      if (it == inflight_.end()) continue;
      auto mid = it->second;
      inflight_.erase(it);
      store_.erase(mid);
      acked_.insert(mid);
    } else if (type == "expire") {
      auto tid = e.at("transit_id").get<std::string>();
      auto it = inflight_.find(tid);
      if (it == inflight_.end()) continue;
      auto mid = it->second;
      inflight_.erase(it);
      auto& msg = store_.at(mid);
      msg.transit_id.reset();
      msg.visible_again_at.reset();
      push_ready(msg);
    }
  }
  expire_and_promote(now);
}

Message QueueEngine::enqueue(const std::string& body, int priority,
                             std::optional<double> delay,
                             std::optional<std::string> message_id) {
  std::lock_guard lock(mu_);
  double now = clock_();
  expire_and_promote(now);
  double effective = delay.value_or(config_.default_delay);
  if (effective < 0) throw std::invalid_argument("delay must be >= 0");
  order_seq_ += 1;
  Message msg;
  msg.id = message_id.value_or(make_id());
  msg.body = body;
  msg.priority = priority;
  msg.order_seq = order_seq_;
  msg.available_at = now + effective;
  append({{"type", "enqueue"},
          {"id", msg.id},
          {"body", msg.body},
          {"priority", msg.priority},
          {"order_seq", msg.order_seq},
          {"available_at", msg.available_at}});
  store_[msg.id] = msg;
  place_new(store_[msg.id], now);
  return store_[msg.id];
}

std::optional<Message> QueueEngine::receive() {
  std::lock_guard lock(mu_);
  double now = clock_();
  expire_and_promote(now);
  auto m = pop_ready();
  if (!m) return std::nullopt;
  auto& msg = **m;
  auto tid = make_id();
  double visible = now + config_.visibility_timeout;
  append({{"type", "receive"},
          {"id", msg.id},
          {"transit_id", tid},
          {"visible_again_at", visible}});
  msg.state = MessageState::InFlight;
  msg.transit_id = tid;
  msg.visible_again_at = visible;
  msg.delivery_count += 1;
  inflight_[tid] = msg.id;
  return msg;
}

bool QueueEngine::ack(const std::string& transit_id) {
  std::lock_guard lock(mu_);
  auto it = inflight_.find(transit_id);
  if (it == inflight_.end()) return false;
  append({{"type", "ack"}, {"transit_id", transit_id}});
  auto mid = it->second;
  inflight_.erase(it);
  store_.erase(mid);
  acked_.insert(mid);
  return true;
}

std::vector<std::string> QueueEngine::tick() {
  std::lock_guard lock(mu_);
  return expire_and_promote(clock_());
}

std::unordered_map<std::string, int> QueueEngine::depths() const {
  std::lock_guard lock(mu_);
  int staged = 0, ready = 0, inflight = 0;
  for (const auto& [_, m] : store_) {
    if (m.state == MessageState::Staged) ++staged;
    else if (m.state == MessageState::Ready) ++ready;
    else if (m.state == MessageState::InFlight) ++inflight;
  }
  return {{"staged", staged},
          {"ready", ready},
          {"in_flight", inflight},
          {"store", static_cast<int>(store_.size())}};
}

}  // namespace queuemaxxing

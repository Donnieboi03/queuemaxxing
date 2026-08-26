#include "queuemaxxing/api.hpp"

#include <memory>
#include <mutex>
#include <unordered_map>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "queuemaxxing/engine.hpp"
#include "queuemaxxing/sweeper.hpp"

namespace queuemaxxing {

struct Registry {
  std::mutex mu;
  std::unordered_map<std::string, std::unique_ptr<QueueEngine>> engines;
  std::filesystem::path root;
  VisibilitySweeper sweeper{std::chrono::milliseconds(100)};

  explicit Registry(std::filesystem::path r) : root(std::move(r)) {
    std::filesystem::create_directories(root);
  }
};

void run_http_server(const std::filesystem::path& data_root, const std::string& host, int port) {
  auto registry = std::make_shared<Registry>(data_root);
  registry->sweeper.start();

  httplib::Server svr;

  svr.Get("/health", [registry](const httplib::Request&, httplib::Response& res) {
    nlohmann::json queues = nlohmann::json::array();
    {
      std::lock_guard lock(registry->mu);
      for (auto& [n, _] : registry->engines) queues.push_back(n);
    }
    res.set_content(nlohmann::json{{"ok", true}, {"queues", queues}}.dump(), "application/json");
  });

  svr.Post("/queues", [registry](const httplib::Request& req, httplib::Response& res) {
    auto body = nlohmann::json::parse(req.body);
    std::string name = body.at("name").get<std::string>();
    OrderMode order = body.value("order", "fifo") == "lifo" ? OrderMode::Lifo : OrderMode::Fifo;
    QueueConfig cfg{name, order, body.value("default_delay", 0.0),
                    body.value("visibility_timeout", 30.0)};
    std::lock_guard lock(registry->mu);
    if (registry->engines.count(name)) {
      res.status = 409;
      res.set_content(R"({"detail":"exists"})", "application/json");
      return;
    }
    auto eng = std::make_unique<QueueEngine>(cfg, registry->root / name);
    registry->sweeper.add(eng.get());
    registry->engines[name] = std::move(eng);
    res.set_content(nlohmann::json{{"name", name},
                                   {"order", order == OrderMode::Fifo ? "fifo" : "lifo"},
                                   {"default_delay", cfg.default_delay},
                                   {"visibility_timeout", cfg.visibility_timeout}}
                        .dump(),
                    "application/json");
  });

  svr.Post(R"(/queues/(\w+)/messages)", [registry](const httplib::Request& req, httplib::Response& res) {
    auto name = req.matches[1];
    auto body = nlohmann::json::parse(req.body);
    std::lock_guard lock(registry->mu);
    auto it = registry->engines.find(name);
    if (it == registry->engines.end()) {
      res.status = 404;
      return;
    }
    std::optional<double> delay;
    if (body.contains("delay") && !body["delay"].is_null()) delay = body["delay"].get<double>();
    auto msg = it->second->enqueue(body.at("body").get<std::string>(), body.value("priority", 0),
                                   delay);
    res.set_content(nlohmann::json{{"message_id", msg.id},
                                   {"priority", msg.priority},
                                   {"order_seq", msg.order_seq},
                                   {"available_at", msg.available_at}}
                        .dump(),
                    "application/json");
  });

  svr.Post(R"(/queues/(\w+)/receive)", [registry](const httplib::Request& req, httplib::Response& res) {
    auto name = req.matches[1];
    std::lock_guard lock(registry->mu);
    auto it = registry->engines.find(name);
    if (it == registry->engines.end()) {
      res.status = 404;
      return;
    }
    auto msg = it->second->receive();
    if (!msg) {
      res.status = 204;
      return;
    }
    res.set_content(nlohmann::json{{"message_id", msg->id},
                                   {"transit_id", *msg->transit_id},
                                   {"body", msg->body},
                                   {"priority", msg->priority},
                                   {"delivery_count", msg->delivery_count}}
                        .dump(),
                    "application/json");
  });

  svr.Post(R"(/queues/(\w+)/ack)", [registry](const httplib::Request& req, httplib::Response& res) {
    auto name = req.matches[1];
    auto body = nlohmann::json::parse(req.body);
    std::lock_guard lock(registry->mu);
    auto it = registry->engines.find(name);
    if (it == registry->engines.end()) {
      res.status = 404;
      return;
    }
    if (!it->second->ack(body.at("transit_id").get<std::string>())) {
      res.status = 404;
      res.set_content(R"({"detail":"stale"})", "application/json");
      return;
    }
    res.set_content(R"({"acked":true})", "application/json");
  });

  svr.listen(host, port);
  registry->sweeper.stop();
}

}  // namespace queuemaxxing

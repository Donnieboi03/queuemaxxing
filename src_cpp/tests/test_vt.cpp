#include <catch2/catch_test_macros.hpp>

#include <filesystem>

#include "queuemaxxing/engine.hpp"

using namespace queuemaxxing;

struct FakeClock {
  double t{1000.0};
  double operator()() const { return t; }
  void advance(double dt) { t += dt; }
};

TEST_CASE("vt redelivery and stale ack") {
  FakeClock clock;
  auto dir = std::filesystem::temp_directory_path() / "qm_vt_test";
  std::filesystem::remove_all(dir);
  QueueEngine eng(QueueConfig{"vt", OrderMode::Fifo, 0, 5}, dir,
                  [&] { return clock(); }, true);
  eng.enqueue("work");
  auto r1 = eng.receive();
  REQUIRE(r1);
  auto t1 = *r1->transit_id;
  REQUIRE_FALSE(eng.receive());
  clock.advance(5);
  auto red = eng.tick();
  REQUIRE_FALSE(red.empty());
  auto r2 = eng.receive();
  REQUIRE(r2);
  REQUIRE(r2->id == r1->id);
  REQUIRE(*r2->transit_id != t1);
  REQUIRE(r2->delivery_count == 2);
  REQUIRE_FALSE(eng.ack(t1));
  REQUIRE(eng.ack(*r2->transit_id));
}

TEST_CASE("wal restart preserves ready") {
  FakeClock clock;
  auto dir = std::filesystem::temp_directory_path() / "qm_restart_test";
  std::filesystem::remove_all(dir);
  {
    QueueEngine eng(QueueConfig{"w", OrderMode::Fifo, 0, 30}, dir,
                    [&] { return clock(); }, true);
    eng.enqueue("one", 1);
    eng.enqueue("two", 5);
  }
  auto eng2 = QueueEngine::open(dir, [&] { return clock(); });
  auto r = eng2.receive();
  REQUIRE(r);
  REQUIRE(r->body == "two");
}

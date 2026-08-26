#include <catch2/catch_test_macros.hpp>

#include "queuemaxxing/engine.hpp"

using namespace queuemaxxing;

struct FakeClock {
  double t{1000.0};
  double operator()() const { return t; }
  void advance(double dt) { t += dt; }
};

TEST_CASE("fifo same priority") {
  FakeClock clock;
  QueueEngine eng(QueueConfig{"t", OrderMode::Fifo, 0, 30}, std::nullopt,
                  [&] { return clock(); }, false);
  auto a = eng.enqueue("a", 1);
  auto b = eng.enqueue("b", 1);
  auto r1 = eng.receive();
  auto r2 = eng.receive();
  REQUIRE(r1);
  REQUIRE(r2);
  REQUIRE(r1->id == a.id);
  REQUIRE(r2->id == b.id);
}

TEST_CASE("lifo same priority") {
  FakeClock clock;
  QueueEngine eng(QueueConfig{"t", OrderMode::Lifo, 0, 30}, std::nullopt,
                  [&] { return clock(); }, false);
  auto a = eng.enqueue("a", 1);
  auto b = eng.enqueue("b", 1);
  auto r1 = eng.receive();
  REQUIRE(r1);
  REQUIRE(r1->id == b.id);
  auto r2 = eng.receive();
  REQUIRE(r2);
  REQUIRE(r2->id == a.id);
}

TEST_CASE("priority beats order") {
  FakeClock clock;
  QueueEngine eng(QueueConfig{"t", OrderMode::Fifo, 0, 30}, std::nullopt,
                  [&] { return clock(); }, false);
  eng.enqueue("low", 1);
  auto high = eng.enqueue("high", 10);
  auto r = eng.receive();
  REQUIRE(r);
  REQUIRE(r->id == high.id);
}

TEST_CASE("delay hides until available") {
  FakeClock clock;
  QueueEngine eng(QueueConfig{"t", OrderMode::Fifo, 0, 30}, std::nullopt,
                  [&] { return clock(); }, false);
  auto msg = eng.enqueue("later", 0, 5.0);
  REQUIRE_FALSE(eng.receive());
  REQUIRE(eng.depths().at("staged") == 1);
  clock.advance(5);
  auto r = eng.receive();
  REQUIRE(r);
  REQUIRE(r->id == msg.id);
}

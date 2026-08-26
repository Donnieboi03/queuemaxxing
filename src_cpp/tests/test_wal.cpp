#include <catch2/catch_test_macros.hpp>

#include <filesystem>

#include "queuemaxxing/wal.hpp"

using namespace queuemaxxing;

TEST_CASE("wal append and load monotonic") {
  auto dir = std::filesystem::temp_directory_path() / "qm_wal_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  Wal wal(dir / "queue.wal");
  wal.append({{"type", "enqueue"}, {"id", "a"}});
  wal.append({{"type", "enqueue"}, {"id", "b"}});
  auto events = wal.load();
  REQUIRE(events.size() == 2);
  REQUIRE(events[0]["seq"] == 1);
  REQUIRE(events[1]["seq"] == 2);
  REQUIRE(wal.last_seq_on_disk() == 2);
}

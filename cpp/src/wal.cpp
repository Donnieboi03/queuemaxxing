#include "queuemaxxing/wal.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <stdexcept>
#include <system_error>

namespace queuemaxxing {

Wal::Wal(std::filesystem::path path) : path_(std::move(path)) {
  if (path_.has_parent_path()) {
    std::filesystem::create_directories(path_.parent_path());
  }
  if (std::filesystem::exists(path_)) {
    for (const auto& e : load()) {
      if (e.contains("seq")) seq_ = std::max(seq_, e["seq"].get<std::int64_t>());
    }
  }
}

nlohmann::json Wal::append(nlohmann::json event) {
  seq_ += 1;
  event["v"] = 1;
  event["seq"] = seq_;
  std::ofstream out(path_, std::ios::app);
  if (!out) throw std::runtime_error("failed to open wal for append");
  out << event.dump() << '\n';
  out.flush();
  out.close();
  int fd = ::open(path_.c_str(), O_RDONLY);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }
  return event;
}

std::vector<nlohmann::json> Wal::load() const {
  std::vector<nlohmann::json> out;
  if (!std::filesystem::exists(path_)) return out;
  std::ifstream in(path_);
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    out.push_back(nlohmann::json::parse(line));
  }
  return out;
}

std::int64_t Wal::last_seq_on_disk() const {
  std::int64_t last = 0;
  std::int64_t prev = 0;
  for (const auto& e : load()) {
    auto s = e.at("seq").get<std::int64_t>();
    if (s <= prev) throw std::runtime_error("WAL seq not monotonic");
    prev = s;
    last = s;
  }
  return last;
}

}  // namespace queuemaxxing

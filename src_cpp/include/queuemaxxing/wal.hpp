#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace queuemaxxing {

class Wal {
 public:
  explicit Wal(std::filesystem::path path);

  std::int64_t seq() const { return seq_; }
  const std::filesystem::path& path() const { return path_; }

  nlohmann::json append(nlohmann::json event);
  std::vector<nlohmann::json> load() const;
  std::int64_t last_seq_on_disk() const;

 private:
  std::filesystem::path path_;
  std::int64_t seq_{0};
};

}  // namespace queuemaxxing

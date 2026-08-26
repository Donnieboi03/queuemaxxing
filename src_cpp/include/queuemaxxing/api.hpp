#pragma once

#include <filesystem>
#include <string>

namespace queuemaxxing {

void run_http_server(const std::filesystem::path& data_root, const std::string& host, int port);

}  // namespace queuemaxxing

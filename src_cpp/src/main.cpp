#include "queuemaxxing/api.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  int port = 8081;
  std::filesystem::path data = "tmp/data-cpp";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--port" && i + 1 < argc) port = std::atoi(argv[++i]);
    else if (a == "--host" && i + 1 < argc) host = argv[++i];
    else if (a == "--data" && i + 1 < argc) data = argv[++i];
  }
  std::cout << "queuemaxxing_cpp listening on " << host << ":" << port
            << " data=" << data << "\n";
  queuemaxxing::run_http_server(data, host, port);
  return 0;
}

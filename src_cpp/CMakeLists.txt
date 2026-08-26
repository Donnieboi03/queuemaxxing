cmake_minimum_required(VERSION 3.20)
project(queuemaxxing_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG v3.5.4
)
FetchContent_Declare(
  json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3
)
FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG v0.16.0
)
FetchContent_MakeAvailable(Catch2 json httplib)

add_library(queuemaxxing_core
  src/engine.cpp
  src/wal.cpp
  src/sweeper.cpp
)
target_include_directories(queuemaxxing_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(queuemaxxing_core PUBLIC nlohmann_json::nlohmann_json)

add_executable(queuemaxxing_cpp src/main.cpp src/api.cpp)
target_link_libraries(queuemaxxing_cpp PRIVATE queuemaxxing_core httplib::httplib)

add_executable(queuemaxxing_stress src/stress.cpp)
target_link_libraries(queuemaxxing_stress PRIVATE queuemaxxing_core)

enable_testing()
add_executable(queuemaxxing_tests
  tests/test_engine.cpp
  tests/test_wal.cpp
  tests/test_vt.cpp
)
target_link_libraries(queuemaxxing_tests PRIVATE queuemaxxing_core Catch2::Catch2WithMain)
include(CTest)
include(Catch)
catch_discover_tests(queuemaxxing_tests)

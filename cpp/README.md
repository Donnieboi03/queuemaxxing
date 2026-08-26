# Queuemaxxing C++

Second implementation of the frankenstein queue (same design as Python).

**Branch:** `feat/cpp-port`  
**Primary submit path:** Python at repo root.

## Build

```bash
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build -j
ctest --test-dir cpp/build --output-on-failure
```

## Binaries

```bash
# HTTP server (default port 8081)
./cpp/build/queuemaxxing_cpp --port 8081 --data tmp/data-cpp

# Throughput stress → tmp/stress-cpp-*.json
./cpp/build/queuemaxxing_stress --messages 20000 --producers 8 --consumers 8
./cpp/build/queuemaxxing_stress --messages 5000 --wal
```

## Sample stress (this machine)

- In-mem: ~228k enqueue / ~151k consume msg/s  
- WAL+fsync: ~22k enqueue / ~10k consume msg/s  

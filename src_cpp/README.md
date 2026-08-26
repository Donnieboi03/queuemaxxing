# Queuemaxxing C++ (`src_cpp/`)

Second implementation of the frankenstein queue (same design as Python in `src_py/`).

**Branch:** `feat/cpp-port`  
**Primary submit path:** Python.

## Build

```bash
cmake -S src_cpp -B src_cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build src_cpp/build -j
ctest --test-dir src_cpp/build --output-on-failure
```

## Binaries

```bash
./src_cpp/build/queuemaxxing_cpp --port 8081 --data tmp/data-cpp
./src_cpp/build/queuemaxxing_stress --messages 20000 --producers 8 --consumers 8
./src_cpp/build/queuemaxxing_stress --messages 5000 --wal
```

## Sample stress (vs Python)

| Mode | Enqueue | Consume |
| --- | --- | --- |
| C++ mem | ~228k msg/s | ~151k msg/s |
| C++ WAL+fsync | ~22k msg/s | ~10k msg/s |
| Python mem (typical) | ~3.5k msg/s | ~1.7k msg/s |

Bound by **CPU/lock** without WAL and **fsync** with WAL — not the network. See [DESIGN.md](../DESIGN.md) §Performance.

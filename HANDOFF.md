# Queuemaxxing — agent handoff

Read this before coding. Design authority: [PLAN.md](./PLAN.md), [DESIGN.md](./DESIGN.md).

## Status

| Field | Value |
| --- | --- |
| Phase | **C++ port on `feat/cpp-port`** (Python complete on `main`) |
| Current | C++ engine+WAL+VT+HTTP+stress green |
| Primary submit | Python package |

## Done

- Python frankenstein queue on `main`
- C++ port: staged/ready/in-flight, JSONL WAL, VT sweeper, httplib API, stress CLI
- `ctest` 7/7 passed; stress ~228k/151k msg/s mem, ~22k/10k WAL

## Next

1. Push `feat/cpp-port` when asked
2. Optional merge to `main`
3. Optional Pub/Sub / WAL compaction

## Commands

### Python
```bash
source .venv/bin/activate
pytest tests/unit tests/integration tests/e2e -q
```

### C++
```bash
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release && cmake --build cpp/build -j
ctest --test-dir cpp/build --output-on-failure
./cpp/build/queuemaxxing_stress --messages 20000 --producers 8 --consumers 8
```

## Rules

- Python remains primary for Artie email
- Commit on `feat/cpp-port`; no force-push to `main`

## Blockers

_None._

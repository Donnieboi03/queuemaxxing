# Queuemaxxing — agent handoff

Read this before coding. Design authority: [PLAN.md](./PLAN.md), [DESIGN.md](./DESIGN.md).

## Status

| Field | Value |
| --- | --- |
| Phase | **C++ port on `feat/cpp-port`** (Python complete on `main`) |
| Layout | `src_py/` Python · `src_cpp/` C++ |
| Primary submit | Python |

## Done

- Python frankenstein queue (`src_py/`)
- C++ port (`src_cpp/`): engine, WAL, VT, HTTP, stress
- Layout rename `src`→`src_py`, `cpp`→`src_cpp` for clarity
- Performance notes in DESIGN (C++ mem ~228k/151k vs Python ~3.5k/1.7k)

## Next

1. Push `feat/cpp-port` (in progress)
2. Optional merge to `main`
3. Optional Pub/Sub / WAL compaction

## Commands

### Python
```bash
source .venv/bin/activate && pip install -e ".[dev]"
pytest tests/unit tests/integration tests/e2e -q
python demo/stress.py engine --messages 10000 --producers 4 --consumers 4
```

### C++
```bash
cmake -S src_cpp -B src_cpp/build -DCMAKE_BUILD_TYPE=Release && cmake --build src_cpp/build -j
ctest --test-dir src_cpp/build --output-on-failure
./src_cpp/build/queuemaxxing_stress --messages 20000 --producers 8 --consumers 8
```

## Rules

- Python remains primary for Artie email
- Commit on `feat/cpp-port`; no force-push to `main`

## Blockers

_None._

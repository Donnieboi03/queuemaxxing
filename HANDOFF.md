# Queuemaxxing — agent handoff

Read this before coding. Design authority: [PLAN.md](./PLAN.md), [DESIGN.md](./DESIGN.md).

## Status

| Field | Value |
| --- | --- |
| Phase | **Merged to `main`** — Python + C++ dual runtime |
| Layout | `src_py/` Python · `src_cpp/` C++ |
| Primary submit | Python |

## Done

- Python frankenstein queue (`src_py/`)
- C++ port (`src_cpp/`): engine, WAL, VT, HTTP, stress
- Layout `src_py` / `src_cpp`; performance notes in DESIGN
- `feat/cpp-port` merged into `main`

## Next

1. Optional Pub/Sub topic fan-out demo
2. WAL snapshot compaction
3. Submit when ready

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

- Python remains primary for submission
- No force-push to `main`

## Blockers

_None._

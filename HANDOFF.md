# Queuemaxxing — agent handoff

Read this before coding. Design authority: [PLAN.md](./PLAN.md), [DESIGN.md](./DESIGN.md).

## Status

| Field | Value |
| --- | --- |
| Phase | **Python package complete** (C++ **not** started) |
| Current slice | Stress harness + tmp/ scratch + slice commits |
| Pub/Sub code | Out of scope this phase (see DESIGN.md) |

## Done

- Scaffold: `pyproject.toml`, `src/queuemaxxing/`, `demo/`, tests pyramid, README
- `QueueEngine`: staged time-heap → ready composite heap → in-flight + VT
- JSONL WAL append/fsync + replay / restart
- Visibility sweeper + transit_id redelivery
- Observability: structlog + Prometheus `/metrics` + `/debug/integrity`
- FastAPI MPMC API + producer/consumer demos
- Unit / integration / e2e tests (incl. MPMC stress)
- `tmp/` scratch (gitignored) + `demo/stress.py` throughput harness

## Next (future phases)

1. Optional C++ port (deferred)
2. Optional Pub/Sub topic fan-out demo
3. WAL snapshot compaction
4. Push to GitHub when ready to email [redacted]

## Commands

```bash
cd /Users/donnieb/Desktop/Code/queuemaxxing
source .venv/bin/activate   # or: python -m venv .venv && pip install -e ".[dev]"
pytest tests/unit tests/integration tests/e2e -q

QUEUEMAXXING_DATA=tmp/data queuemaxxing --port 8080
python demo/producer.py --queue demo
python demo/consumer.py --queue demo --seconds 15

python demo/stress.py engine --messages 10000 --producers 4 --consumers 4
python demo/stress.py engine --messages 5000 --wal
python demo/stress.py http --messages 2000 --producers 4 --consumers 4
```

## Rules for agents

- Do **not** start C++ or Pub/Sub topic implementation unless a new plan says so
- Keep integrity auditor green after engine changes
- Put local/stress artifacts under `tmp/` (not in git)
- Update this file after meaningful work

## Last tests

```text
pytest tests/unit tests/integration tests/e2e -q
23 passed (plus throughput smoke once landed)
```

## Blockers

_None._

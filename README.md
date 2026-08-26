# Queuemaxxing

Artie take-home: a composable durable “Frankenstein” queue over HTTP.

**Primary implementation: Python** (`src/queuemaxxing/`).  
**C++ port (in progress on `feat/cpp-port`):** see [cpp/README.md](./cpp/README.md).

| Knob | Meaning |
| --- | --- |
| **Order** | FIFO or LIFO |
| **Priority** | Higher priority dequeues first |
| **Delay** | Invisible until `available_at` |

## Quick start

```bash
cd queuemaxxing
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# tests
pytest tests/unit tests/integration tests/e2e -q

# server (WAL under ./data by default)
queuemaxxing --port 8080
# or local scratch: QUEUEMAXXING_DATA=tmp/data queuemaxxing --port 8080
```

### Demo (MPMC)

```bash
# terminal 1
QUEUEMAXXING_DATA=tmp/data queuemaxxing --port 8080

# terminal 2
python demo/producer.py --queue demo --producers 4 --each 25

# terminal 3
python demo/consumer.py --queue demo --consumers 4 --seconds 15
```

### Throughput stress

Writes reports under `tmp/stress-*.json` (gitignored scratch; see `tmp/README.md`).

```bash
# in-process engine (memory)
python demo/stress.py engine --messages 10000 --producers 4 --consumers 4

# engine + durable WAL under tmp/data
python demo/stress.py engine --messages 5000 --producers 4 --consumers 4 --wal

# HTTP API (TestClient, no separate server process)
python demo/stress.py http --messages 2000 --producers 4 --consumers 4
```

### HTTP

- `POST /queues` — create `{ name, order, default_delay, visibility_timeout }`
- `POST /queues/{name}/messages` — `{ body, priority?, delay? }`
- `POST /queues/{name}/receive?wait_seconds=0` — message or `204`
- `POST /queues/{name}/ack` — `{ transit_id }`
- `GET /health`, `GET /metrics`, `GET /debug/integrity`

## Docs

| Doc | Role |
| --- | --- |
| [PLAN.md](./PLAN.md) | Design thought process |
| [DESIGN.md](./DESIGN.md) | Architecture + Artie Q&A |
| [HANDOFF.md](./HANDOFF.md) | Agent progress / next slice |
| [tmp/README.md](./tmp/README.md) | Local scratch / stress artifacts |

## Constraints

- Durable local JSONL WAL (no Redis/Postgres/SQS)
- MPMC-safe (`threading` lock per queue + HTTP worker pool)
- C++ port / Pub/Sub code deferred (see DESIGN for Pub/Sub write-up)

# Queuemaxxing

Artie take-home: a composable durable “Frankenstein” queue over HTTP.

| Layout | Language |
| --- | --- |
| [`src_py/`](./src_py/) | **Primary** Python package |
| [`src_cpp/`](./src_cpp/) | C++ port — see [src_cpp/README.md](./src_cpp/README.md) |

| Knob | Meaning |
| --- | --- |
| **Order** | FIFO or LIFO |
| **Priority** | Higher priority dequeues first |
| **Delay** | Invisible until `available_at` |

## Quick start (Python)

```bash
cd queuemaxxing
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest tests/unit tests/integration tests/e2e -q

queuemaxxing --port 8080
# or: QUEUEMAXXING_DATA=tmp/data queuemaxxing --port 8080
```

### Demo (MPMC)

```bash
QUEUEMAXXING_DATA=tmp/data queuemaxxing --port 8080
python demo/producer.py --queue demo --producers 4 --each 25
python demo/consumer.py --queue demo --consumers 4 --seconds 15
```

### Throughput stress

```bash
# Python
python demo/stress.py engine --messages 10000 --producers 4 --consumers 4
python demo/stress.py engine --messages 5000 --wal
python demo/stress.py http --messages 2000 --producers 4 --consumers 4

# C++
cmake -S src_cpp -B src_cpp/build -DCMAKE_BUILD_TYPE=Release && cmake --build src_cpp/build -j
./src_cpp/build/queuemaxxing_stress --messages 20000 --producers 8 --consumers 8
./src_cpp/build/queuemaxxing_stress --messages 5000 --wal
```

Reports land under `tmp/` (gitignored). Speed notes: [DESIGN.md](./DESIGN.md) §Performance.

### HTTP (Python)

- `POST /queues` — `{ name, order, default_delay, visibility_timeout }`
- `POST /queues/{name}/messages` — `{ body, priority?, delay? }`
- `POST /queues/{name}/receive?wait_seconds=0` — message or `204`
- `POST /queues/{name}/ack` — `{ transit_id }`
- `GET /health`, `GET /metrics`, `GET /debug/integrity`

## Docs

| Doc | Role |
| --- | --- |
| [PLAN.md](./PLAN.md) | Design thought process + C++ parity |
| [DESIGN.md](./DESIGN.md) | Architecture, performance, Artie Q&A |
| [HANDOFF.md](./HANDOFF.md) | Agent progress |
| [tmp/README.md](./tmp/README.md) | Local scratch |

## Constraints

- Durable local JSONL WAL (no Redis/Postgres/SQS)
- MPMC-safe (`threading` / `std::mutex` per queue)
- Python is the primary submission; C++ is a same-design systems port

# Design

Canonical architecture for Queuemaxxing. Thought process, discarded ideas, and
open concerns live in [PLAN.md](./PLAN.md).

## Model

One composable queue with orthogonal knobs:

| Knob | Implementation |
| --- | --- |
| **FIFO / LIFO** | Temporal `order_seq` tie-break on the ready heap |
| **Priority** | Primary key on the ready heap |
| **Delay** | `available_at`; message sits in **staged** until due |

### Implementations

| Runtime | Role | Location |
| --- | --- | --- |
| **Python** | Primary submission (complete on `main`) | `src_py/queuemaxxing/` |
| **C++** | Same design, second implementation (`feat/cpp-port`) | `src_cpp/` |

Both use staged → ready → in-flight + local JSONL WAL + VT leases. WAL formats need not be byte-identical across languages.

### Performance (same machine, indicative)

| Mode | Enqueue msg/s | Consume msg/s | What dominates |
| --- | --- | --- | --- |
| Python engine (mem) | ~3.5k | ~1.7k | Lock + interpreter + logging/metrics |
| Python engine (WAL+fsync) | ~few k | ~few k | fsync + Python |
| C++ engine (mem, 20k/8×8) | ~228k | ~151k | Lock + heap ops (CPU) |
| C++ engine (WAL+fsync) | ~22k | ~10k | Disk fsync |

**Takeaway:** the design is **not network-bound** in these benches. In-memory C++ shows the lock/CPU ceiling; with WAL both languages are **fsync/I/O-bound**, but C++ still clears Python by roughly an order of magnitude here. HTTP TestClient benches sit below raw engine rates because of framework overhead, not the NIC.

Queue default delay + optional per-message override:

`effective_delay = message.delay ?? queue.default_delay`

### Lanes

```text
STAGED (time) → READY (priority + seq) → IN-FLIGHT (transit_id + VT) → ack
                                      ↖________ redelivery ________↙
```

- **Staged:** min-heap by `available_at` (v1). Optional optimization: deque when
  only uniform queue-level delay is allowed ([PLAN.md](./PLAN.md) §3.3).
- **Ready:** one heap keyed `(priority, order_seq)` — not heap+deque.
- **In-flight:** lease after receive; visibility timeout enables replay.
- **Store:** `id → message`; heaps hold handles.
- **Concurrency:** **MPMC** at the API (many producers + many consumers);
  one mutex per queue; shared HTTP worker pool. Optional sweeper for VT /
  next-due — not one OS thread per lane.
- **Durability:** local append-only JSONL WAL (no Redis/Postgres/SQS). Rebuild
  lanes on restart by replay.

Consumers receive payloads **by value** over HTTP plus `message_id` /
`transit_id`. Redelivery ⇒ at-least-once; consumers should be idempotent.

## Additional questions

### How do you handle replay messages?

A `receive` moves a message to **in-flight** and issues a **transit_id** with
`visible_again_at = now + visibility_timeout`. If the client **acks** that
transit id, the message is deleted. If the timeout elapses without a valid ack
(crash, slow worker, lost response), the message returns to **ready** and may
be received again with a **new** transit_id. That is intentional
**at-least-once** redelivery (“replay”), not a separate replay API. Callers
dedupe on `message_id` (or their own idempotency key). Stale acks for expired
transit ids are ignored so they cannot delete a newer lease.

Delay is unrelated: it only postpones **first** eligibility via `available_at`.

### How would you refactor into Pub/Sub?

Keep the durable frankenstein **engine**; change topology at **publish** time.

- A **topic** is only a name + set of subscriptions (no shared staged/ready).
- Each **subscriber** (logical consumer that wants its own copy — e.g. email,
  inventory) gets a **subscription** = a full queue instance (staged / ready /
  in-flight, same knobs).
- `publish` **fan-outs** a copy into every subscription queue immediately.
- Subscribers **poll** (or long-poll) `receive`/`ack` on **their** subscription
  — transit/VT stays per subscription, not the fan-out moment.

Competing workers on one queue ≠ Pub/Sub. Pub/Sub = broadcast across
subscriptions; optional competing consumers **inside** one subscription.

**Option B (later):** one log + per-subscriber cursors (Pulsar/Kafka-like).

This refactor is answered in docs for the take-home; implementing topics is
optional extra credit after the core queue ships.

### If you had more time, what other features would you add?

- WAL **snapshot + compaction**
- Dead-letter queue after N delivery attempts
- Per-receive VT override; delay longer than “demo scale”
- Metrics (depth, age, in-flight, promote rate), admin inspect API
- Sharded locks for throughput; optional deque staged path
- Poison-message detection; scheduled messages calendar UI in the demo app
- Careful exactly-once *session* patterns on top of at-least-once (outbox)

### Why choose this over SQS, RabbitMQ, or Apache Pulsar?

You wouldn’t replace those for multi-tenant cloud scale. This wins when you want:

- **Embeddable** durability with **zero** external broker/DB ops
- **Composable** FIFO/LIFO + priority + delay in one small process
- A **clear teaching/production-lite** core (WAL, leases, promote) you can read
  in one repo

Incumbents win on managed HA, huge fan-out, rich routing, multi-region, and
operational tooling. This project is the frankenstein kernel and a demo app —
honest about being at-least-once and single-node unless extended.

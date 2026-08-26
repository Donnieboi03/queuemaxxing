# Queuemaxxing — plan & thought process

How we arrived at the design: **solutions first**, and **why each piece felt
right**. Clean API/architecture summary also lives in [DESIGN.md](./DESIGN.md).

---

## 1. What we’re building

An HTTP “Frankenstein” queue: one engine whose delivery policy is composed from

| Knob | Meaning |
| --- | --- |
| **Order** | FIFO or LIFO |
| **Priority** | Higher priority dequeues first |
| **Delay** | Invisible until `available_at` |

So the same codebase can present a **priority FIFO**, a **delay + priority
LIFO**, and so on. Plus a small demo app, local durability across restarts,
concurrency, and short answers on replay / Pub/Sub / roadmap / vs incumbents.

---

## 2. Core insight: orthogonal knobs on one queue

We read the brief as **one composable queue**, not three separate products.
“Frankenstein” means the knobs **combine**.

We also separated two different meanings of “time”:

- **FIFO / LIFO** → *temporal arrival* among peers (`order_seq`)
- **Priority** → *importance*, independent of when the message showed up
- **Delay** → *first eligibility* (`available_at`), before it ever competes

That led naturally to a **layered lifecycle**: hold until due, then compete on
priority + order, then lease to a consumer.

```text
STAGED (time) → READY (priority + seq) → IN-FLIGHT (lease) → ack
                                      ↖____ redelivery ____↙
```

---

## 3. Solutions and why we thought of them

### 3.1 Ready lane = one composite heap

**Solution:** a single ready heap ordered by `(priority, order_seq)`, flipping
the `order_seq` direction for LIFO vs FIFO.

**Why:** we want “who’s next?” in one pop. Priority is the primary key; arrival
order is only a tie-break. Encoding both in the comparator keeps **one**
structure, O(log n) enqueue/dequeue, and a clear story: temporal policy is a
sort key, not a second list to keep in sync.

We briefly pictured a heap *plus* a deque for “first/last temporal in O(1),”
then realized temporal identity already lives in `order_seq` on the heap node.
The heap alone carries both dimensions we need on the ready path.

### 3.2 Staged lane = time-ordered “waiting room”

**Solution:** messages with `available_at > now` live in **staged**, keyed only
by time. When due, they **promote** into ready and only then compete on
priority.

**Why delay exists (motivation):** schedule work without parking it in app
memory — retry backoff, “run in N seconds,” cooling-off, soft dependencies.
SQS’s [delay queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-delay-queues.html)
match this: hide on enqueue; consumers simply don’t see the message yet.

**Why a separate lane:** delay is a **gate**, not another priority. Keeping
staged disjoint from ready makes promote a simple reindex: same message id,
new membership.

### 3.3 Dual-stream staged (deque *or* time heap)

**Solution:** pick the staged structure from delay policy:

| Policy | Staged structure | Why it fits |
| --- | --- | --- |
| Uniform queue-level delay | **Deque — O(1)** | Same D for everyone ⇒ due order = enqueue order |
| Per-message delays | **Min-heap — O(log n)** by `available_at` | Different delays ⇒ soonest-due may not be front of a FIFO |

**Why we thought of both:** maximize the simple case (global delay feels like a
queue knob in the brief) without giving up **granularity** when a producer
passes a per-node delay (SQS-style message timers). Mid-life policy change can
**reshape** deque → heap (or we start on heap always).

**v1 preference:** always use the time min-heap — one path, still correct when
all delays are equal; treat the deque path as a documented optimization.

### 3.4 Delay defaults: per-node with queue-level fallback

**Solution:**

```text
effective_delay = message.delay ?? queue.default_delay
available_at    = enqueued_at + effective_delay
```

**Why:** one code path covers “this queue delays everything by 5s” and “this
message waits 60s.” Queue-level delay is just the default every node inherits.
Matches how we’d want to operate it day to day.

### 3.5 Promote + receive under one lock

**Solution:** each mutating API takes a **per-queue mutex**. `receive` does:

1. Lock  
2. Promote every staged message with `available_at <= now` into ready  
3. Pop the ready root  
4. Place it in-flight, append WAL  
5. Unlock  

**Why:** when a delay quantum fires, many messages can become eligible at once
(“influx”). We want that influx applied **before** choosing the next message,
so priority among newly due work is respected. The lock is the mechanism:
promote and take are one atomic story; waiters queue on the mutex instead of
observing a half-migrated ready set.

Same-set promote order may change heap **shape**; the **pop sequence** stays
defined by the comparator — that’s enough for correctness.

### 3.6 In-flight, transit_id, visibility timeout

**Solution:** on receive, assign a **transit_id** (delivery attempt / receipt),
set `visible_again_at = now + VT`, return body **by value** over HTTP. Ack
commits deletion; VT expiry returns the message to ready with a **new**
transit_id.

**Why we added this though the three knobs don’t name it:**

- **Concurrency:** two consumers need a lease so they don’t both “own” the same
  work.
- **Replay question:** redelivery after a missed ack *is* the replay story
  (at-least-once).
- **Idempotency:** clients key off `message_id`; transit_id scopes *this*
  attempt so a late ack can’t clobber a newer lease.

Mental split we liked (same as SQS):

| Mechanism | When it hides the message | Intent |
| --- | --- | --- |
| **Delay** | After enqueue | First eligibility |
| **VT** | After receive | Lease / retry |

### 3.7 Identity map (handles, not LRU)

**Solution:** `id → message` store; heaps and in-flight tables hold **handles**.

**Why:** payloads stay in one place; promote / receive / ack are reindexes.
We talked about a cache-like layer for “reference by id” — the useful part is
the map; eviction (LRU) isn’t part of queue semantics, so we keep a plain store.

### 3.8 Durability = local append-only WAL (JSONL)

**Solution:** every state change appends one **row** to a local log; memory is
the fast index; restart **replays** the log into staged / ready / in-flight.

**Why this shape:** the brief wants durability *and* forbids delegating storage
to a separate queue or database. A file we own keeps the story honest and
reviewable (`tail` the log). JSONL is easy to version (`v:1`), debug, and
extend.

**Envelope:**

```json
{"v":1,"seq":1,"ts":...,"type":"enqueue","id":"m1","priority":10,"order_seq":1,"available_at":...,"body":"..."}
```

| Type | Role |
| --- | --- |
| `queue_meta` | order mode, default delay, VT |
| `enqueue` | create message + place staged or ready |
| `receive` | transit_id + visible_again_at |
| `ack` | finish delivery |
| `expire` | optional explicit VT → ready (or derive on replay) |
| `snapshot` | later compaction |

**Write path we want:** append (+ fsync policy) **then** update memory so a
crash leaves a prefix of the log that replay can apply idempotently via
`last_applied_seq`.

**Later:** snapshot + truncate so replay stays bounded.

### 3.9 Scaling story (when we outgrow one lock)

**Solution for the take-home:** one mutex per queue — clarity over peak QPS.

**If we push throughput later:** **shard by queue** (or key), each shard with
its own staged/ready/lock/WAL segment. That preserves the promote invariant
locally without inventing cross-structure lock-free heaps on day one.

### 3.10 Concurrency = MPMC at the API

**Solution:** treat the brief’s “must support concurrency” as **multi-producer,
multi-consumer (MPMC)** on the queue API — many clients may enqueue and many
may receive at once, safely.

**Why that reading:** a take-home queue that only allows one producer or one
consumer is too weak; HTTP clients will hammer produce and consume together.
MPMC here means **who may call the API**, not “we must run lock-free rings.”

Competing consumers on **one** queue still share one staged/ready: only **one**
worker wins each message (work queue). That is MPMC **queue** semantics.

### 3.11 Threads = shared worker pool, not one hot thread per lane

**Solution:** staged / ready / in-flight stay **data structures** under a
per-queue lock. OS/async concurrency comes from:

| Role | How we run it |
| --- | --- |
| Produce + consume + ack | **HTTP/RPC worker pool** (any worker may hit any queue) |
| VT expiry (± next-due wakeup) | **0 or 1 sweeper/timer** — sparse, not rewriting every node |
| WAL | Sync append on the request path, or **0 or 1** flusher if we batch fsync |

**Why we dropped “staged thread / ready thread / …”:** independent hot threads
per lane add handoffs and still need locking for promote. The pool already
supplies MPMC; the mutex keeps promote+receive atomic. A sweeper only walks
**expired leases** (and optionally sleeps until the next `available_at`) so
redelivery happens even when idle — optional if every `receive` runs
expire+promote first.

No hard cap like “exactly 4 threads”; budget is **N workers + 0..2 helpers**.

### 3.12 Pub/Sub refactor = fan-out at publish, engine per subscription

The brief asks how we’d **refactor into Pub/Sub**. That is a **design
extension** of the same kernel, not a second product for day one.

**Queue (what we build first):**

```text
many pubs + many consumers → one queue’s staged/ready
→ competing receive (one winner per message)
```

**Pub/Sub (write-up / later):**

```text
topic  (name + list of subscriptions only — no shared ready)

subscribe(topic, "email")      → creates email’s frankenstein queue
subscribe(topic, "inventory")  → creates inventory’s frankenstein queue

publish(topic, msg)            → fan-out AT PUBLISH TIME
    ├─ enqueue copy → email staged/ready
    └─ enqueue copy → inventory staged/ready

email polls     POST …/subscriptions/email/receive
inventory polls POST …/subscriptions/inventory/receive
```

**Who are subscribers?** Logical consumers that each want a **copy** of every
event (e.g. email service, inventory service) — not OS threads. Each
subscription gets its **own** staged → ready → in-flight (same knobs, WAL,
transit_id, VT). Slow inventory must not block email.

**Not** staged/ready per **topic** (that would again be competing consumers on
one lane). **Not** fan-out at “end of transit” — transit is per subscription
*after* that sub has received its copy. Polling (or long-poll) on the
**subscription** queue is how the end user drains work; optional push later.

**Why this shape:** reuses the frankenstein engine as a building block; Pub/Sub
becomes a thin router + N queues. Alternate later: one log + per-subscriber
cursors (Pulsar/Kafka-like).

---

## 4. Architecture snapshot

```text
                    ┌─────────────────────────────────────────┐
  enqueue           │  STORE: id → message                     │
  (priority,        │  payload, priority, order_seq,           │
   delay?)          │  available_at, state                     │
        │           └─────────────────────────────────────────┘
        ▼
  ┌─────────────┐   promote due    ┌──────────────┐  receive   ┌────────────┐
  │   STAGED    │ ───────────────► │    READY     │ ─────────► │ IN-FLIGHT  │
  │ time order  │                  │ pri + seq    │            │ transit_id │
  │ deque|heap  │                  │ one heap     │ ◄─ VT ──── │ + VT       │
  └─────────────┘                  └──────────────┘            └─────┬──────┘
                                                                     │ ack
                                                                     ▼
                                                                   DELETED
```

---

## 5. HTTP + demo app

**Core queue (implement):**

- `POST /queues` — `{ name, order, default_delay, visibility_timeout }`
- `POST /queues/:id/messages` — `{ body, priority?, delay? }`
- `POST /queues/:id/receive` — `{ message_id, transit_id, body, … }`
  (optional long-poll / short wait)
- `POST /queues/:id/ack` — `{ transit_id }`

Demo: multi-producer + multi-consumer scripts for priority, delay, FIFO/LIFO,
restart, and redelivery when ack is skipped.

**Pub/Sub surface (design / optional later — not required to ship day one):**

- `POST /topics` / `POST /topics/:t/subscriptions` — register subscriber name
- `POST /topics/:t/publish` — fan-out into each subscription queue
- `POST /subscriptions/:s/receive` + `ack` — poll that sub’s engine

---

## 6. Take-home write-ups (see DESIGN.md)

| Question | Our direction |
| --- | --- |
| Replay | VT + new transit_id; at-least-once; idempotent consumers |
| Pub/Sub | Publish-time fan-out → **per-subscription** frankenstein queues; poll receive; topic has no shared ready |
| More time | Compaction, DLQ, metrics, shards, long-poll, optional Pub/Sub demo |
| vs SQS/Rabbit/Pulsar | Embeddable frankenstein kernel, zero broker ops — not a cloud replacement |

---

## 7. Build order

1. Store + ready composite heap + mutex (MPMC via HTTP pool)  
2. Staged time heap + promote-on-receive  
3. JSONL WAL + restart replay  
4. In-flight + transit_id + VT + ack  
5. HTTP API + multi-producer/multi-consumer demo  
6. Optional: sweeper thread; deque staged path  
7. Docs: Pub/Sub refactor narrative (code optional)  

---

## 8. Choices to lock in while implementing

| Topic | Leaning |
| --- | --- |
| Staged v1 | Always time min-heap; document deque dual-stream as optimization |
| Promote in WAL | Rederive on replay from `available_at` + clock (fewer event types) |
| fsync | Every durable op for the demo; note batched fsync as a knob |
| Storage | WAL files only (strict reading of “no separate database”) |
| Delivery promise | At-least-once + idempotent consumers (no exactly-once claim) |
| Concurrency | MPMC API; one mutex per queue; shared worker pool |
| Background threads | 0 sweeper OK (lazy on receive); +1 sweeper nice for idle VT |
| Pub/Sub in v1 code? | **No** — answer in DESIGN/PLAN; implement only if time left |
| Process shape | Multi-queue in one process is fine; demo can use one |

Language/stack: TBD at implementation time.

---

## 9. Take-home alignment

| Brief ask | Our plan |
| --- | --- |
| FIFO/LIFO + priority + delay, composable | Ready composite heap + staged by time + queue/message delay |
| HTTP app that uses the queue | Queue CRUD + demo producers/consumers |
| Durable, no separate DB/queue | Local JSONL WAL |
| Concurrency | MPMC clients + per-queue mutex + leases (VT) |
| Replay question | VT redelivery + transit_id |
| Pub/Sub question | Written refactor: fan-out → per-sub queues (optional code) |

**In scope to build:** frankenstein **work queue** + durability + concurrent
clients + demo.  
**In scope to explain:** Pub/Sub, roadmap, vs incumbents.  
**Out of scope unless extra time:** full topic/subscription product, per-lane
hot threads, Redis, etc.

---

## 10. One-line summary

**MPMC frankenstein queue: staged by time → ready by priority+seq → in-flight
with transit_id+VT; shared worker pool + per-queue mutex; local JSONL WAL;
Pub/Sub later = publish fan-out into per-subscription copies of that engine.**

---

## 11. C++ port (`feat/cpp-port`)

Python remains the primary Artie path. C++ under `src_cpp/` mirrors the same knobs
and lanes for a systems show-of-effort.

| Item | Status |
| --- | --- |
| Branch | `feat/cpp-port` |
| Layout | `src_py/` (Python) · `src_cpp/` (C++) |
| Engine | staged / ready / in-flight + `std::mutex` |
| WAL | JSONL + fsync (no Python byte-interop required) |
| HTTP | cpp-httplib (`queuemaxxing_cpp`) |
| Stress | `queuemaxxing_stress` → `tmp/stress-cpp-*.json` |

Speed comparison and binding analysis: [DESIGN.md](./DESIGN.md) §Performance.

### Parity checklist

| Capability | Python | C++ |
| --- | --- | --- |
| FIFO / LIFO | yes | yes |
| Priority | yes | yes |
| Delay / staged | yes | yes |
| VT redelivery + stale ack | yes | yes |
| WAL restart | yes | yes |
| MPMC stress | yes | yes |
| HTTP enqueue/receive/ack | yes | yes |
| Prometheus metrics | yes | no (out of scope) |
| Pub/Sub | docs only | no |

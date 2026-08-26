# Queuemaxxing

Artie take-home: a composable “Frankenstein” queue over HTTP, plus a small app that uses it.

## What this is

Build an HTTP service that exposes a **durable, in-process queue** whose delivery semantics can be composed from:

| Knob | Meaning |
| --- | --- |
| **Order** | FIFO or LIFO |
| **Priority** | Higher-priority messages dequeue before lower |
| **Delay** | Message becomes visible only after a delay |

Those combine so you can run e.g. **delay + priority + LIFO**, or **priority + FIFO**, from one implementation.

Then ship a **simple client / demo app** that enqueues, dequeues (or consumes), and exercises those modes.

### Hard constraints

- **Durable across restarts** — crash or restart must not lose messages.
- **No external queue or DB** — persistence must live in this process’s own storage (e.g. local files / embedded store you own). Do not offload to SQS, Redis, Postgres, etc.
- **Concurrency-safe** — multiple clients / goroutines / threads must be able to use the queue safely.

### Design write-ups (expected in this repo)

Answer these in `DESIGN.md` (or expand this README):

1. How do you handle **replay** messages?
2. How would you refactor this into **Pub/Sub**?
3. With more time, what **features** would you add?
4. Why would someone pick this over **SQS / RabbitMQ / Pulsar**?

## Submit

Email the GitHub repo link to **[redacted]**.

## Status

Scaffold only — implementation TBD.

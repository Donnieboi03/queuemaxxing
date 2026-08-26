#!/usr/bin/env python3
"""Multi-producer demo against a running Queuemaxxing server."""

from __future__ import annotations

import argparse
import threading
import time

import httpx


def producer(base: str, queue: str, n: int, priority: int, delay: float) -> None:
    with httpx.Client(base_url=base, timeout=30.0) as client:
        for i in range(n):
            client.post(
                f"/queues/{queue}/messages",
                json={"body": f"job-{priority}-{i}", "priority": priority, "delay": delay},
            )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://127.0.0.1:8080")
    p.add_argument("--queue", default="demo")
    p.add_argument("--producers", type=int, default=4)
    p.add_argument("--each", type=int, default=25)
    p.add_argument("--order", default="fifo", choices=["fifo", "lifo"])
    p.add_argument("--vt", type=float, default=5.0)
    args = p.parse_args()

    with httpx.Client(base_url=args.base, timeout=30.0) as client:
        r = client.post(
            "/queues",
            json={
                "name": args.queue,
                "order": args.order,
                "default_delay": 0,
                "visibility_timeout": args.vt,
            },
        )
        if r.status_code not in (200, 409):
            r.raise_for_status()

    threads = []
    for i in range(args.producers):
        t = threading.Thread(
            target=producer,
            args=(args.base, args.queue, args.each, i, 0.0 if i % 2 == 0 else 0.2),
        )
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print(f"enqueued {args.producers * args.each} messages")


if __name__ == "__main__":
    main()

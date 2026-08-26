#!/usr/bin/env python3
"""Multi-consumer demo against a running Queuemaxxing server."""

from __future__ import annotations

import argparse
import threading
import time

import httpx


def consumer(base: str, queue: str, stop: threading.Event, stats: dict, idx: int) -> None:
    with httpx.Client(base_url=base, timeout=30.0) as client:
        while not stop.is_set():
            r = client.post(f"/queues/{queue}/receive", params={"wait_seconds": 0.5})
            if r.status_code == 204:
                continue
            r.raise_for_status()
            msg = r.json()
            # Simulate work
            time.sleep(0.01)
            ack = client.post(f"/queues/{queue}/ack", json={"transit_id": msg["transit_id"]})
            ack.raise_for_status()
            stats[idx] = stats.get(idx, 0) + 1


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://127.0.0.1:8080")
    p.add_argument("--queue", default="demo")
    p.add_argument("--consumers", type=int, default=4)
    p.add_argument("--seconds", type=float, default=10.0)
    args = p.parse_args()

    stop = threading.Event()
    stats: dict[int, int] = {}
    threads = []
    for i in range(args.consumers):
        t = threading.Thread(target=consumer, args=(args.base, args.queue, stop, stats, i))
        threads.append(t)
        t.start()
    time.sleep(args.seconds)
    stop.set()
    for t in threads:
        t.join()
    print("acked_by_consumer", stats, "total", sum(stats.values()))


if __name__ == "__main__":
    main()

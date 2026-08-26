from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import assert_integrity
from queuemaxxing.models import OrderMode, QueueConfig


def test_throughput_smoke_engine(tmp_path: Path):
    """Small-N throughput smoke (CI-friendly)."""
    logging.disable(logging.WARNING)
    n = 200
    eng = QueueEngine(
        QueueConfig(name="tp", order=OrderMode.FIFO, visibility_timeout=60),
        data_dir=tmp_path,
        durable=True,
    )
    t0 = time.perf_counter()
    for i in range(n):
        eng.enqueue(f"m{i}", priority=i % 5)
    t1 = time.perf_counter()
    acked = 0
    while acked < n:
        msg = eng.receive()
        assert msg is not None
        assert eng.ack(msg.transit_id or "")
        acked += 1
    t2 = time.perf_counter()
    assert_integrity(eng)
    enq_rate = n / (t1 - t0)
    out_rate = n / (t2 - t1)
    # Sanity floors — machine-dependent but should easily clear these.
    assert enq_rate > 50
    assert out_rate > 50


def test_throughput_smoke_mpmc(tmp_path: Path):
    logging.disable(logging.WARNING)
    n = 100
    eng = QueueEngine(
        QueueConfig(name="tp2", order=OrderMode.FIFO, visibility_timeout=60),
        data_dir=tmp_path,
        durable=True,
    )

    def produce() -> None:
        for i in range(n // 2):
            eng.enqueue(f"a{i}")

    t1 = threading.Thread(target=produce)
    t2 = threading.Thread(target=produce)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    got: list[str] = []
    lock = threading.Lock()

    def consume() -> None:
        while True:
            with lock:
                if len(got) >= n:
                    return
            msg = eng.receive()
            if msg is None:
                time.sleep(0.001)
                continue
            eng.ack(msg.transit_id or "")
            with lock:
                got.append(msg.id)

    c1 = threading.Thread(target=consume)
    c2 = threading.Thread(target=consume)
    c1.start()
    c2.start()
    c1.join(timeout=30)
    c2.join(timeout=30)
    assert len(got) == n
    assert_integrity(eng)

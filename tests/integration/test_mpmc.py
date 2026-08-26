from __future__ import annotations

import threading
import time
from pathlib import Path

from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import assert_integrity
from queuemaxxing.models import OrderMode, QueueConfig


def test_mpmc_engine_stress(tmp_path: Path):
    eng = QueueEngine(
        QueueConfig(name="mpmc", order=OrderMode.FIFO, visibility_timeout=60),
        data_dir=tmp_path,
        durable=True,
    )
    n_prod = 4
    n_each = 50
    total = n_prod * n_each
    errors: list[BaseException] = []

    def produce(k: int) -> None:
        try:
            for i in range(n_each):
                eng.enqueue(f"p{k}-{i}", priority=k)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=produce, args=(k,)) for k in range(n_prod)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    got: list[str] = []
    lock = threading.Lock()

    def consume() -> None:
        try:
            while True:
                with lock:
                    if len(got) >= total:
                        return
                msg = eng.receive()
                if msg is None:
                    time.sleep(0.005)
                    continue
                assert eng.ack(msg.transit_id or "")
                with lock:
                    got.append(msg.id)
        except BaseException as e:
            errors.append(e)

    consumers = [threading.Thread(target=consume) for _ in range(4)]
    for t in consumers:
        t.start()
    for t in consumers:
        t.join(timeout=30)

    assert not errors
    assert len(got) == total
    assert len(set(got)) == total
    assert eng.depths()["store"] == 0
    assert_integrity(eng)

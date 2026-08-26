from __future__ import annotations

from pathlib import Path

from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import assert_integrity
from queuemaxxing.models import OrderMode, QueueConfig
from queuemaxxing.sweeper import VisibilitySweeper


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_visibility_timeout_redelivery(tmp_path: Path):
    clock = FakeClock()
    eng = QueueEngine(
        QueueConfig(name="vt", order=OrderMode.FIFO, visibility_timeout=5),
        data_dir=tmp_path,
        clock=clock,
        durable=True,
    )
    eng.enqueue("work")
    r1 = eng.receive()
    assert r1 is not None
    t1 = r1.transit_id
    assert eng.receive() is None
    clock.advance(5)
    red = eng.tick()
    assert r1.id in red
    r2 = eng.receive()
    assert r2 is not None
    assert r2.id == r1.id
    assert r2.transit_id != t1
    assert r2.delivery_count == 2
    assert not eng.ack(t1 or "")
    assert eng.ack(r2.transit_id or "")
    assert_integrity(eng)


def test_sweeper_expires(tmp_path: Path):
    clock = FakeClock()
    eng = QueueEngine(
        QueueConfig(name="vt", order=OrderMode.FIFO, visibility_timeout=1),
        data_dir=tmp_path,
        clock=clock,
        durable=True,
    )
    eng.enqueue("x")
    eng.receive()
    clock.advance(1)
    sweeper = VisibilitySweeper([eng], interval=0.05)
    sweeper.start()
    import time

    time.sleep(0.2)
    sweeper.stop()
    r = eng.receive()
    assert r is not None
    assert r.delivery_count >= 2

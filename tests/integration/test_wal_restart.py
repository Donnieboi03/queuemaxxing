from __future__ import annotations

from pathlib import Path

from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import assert_integrity
from queuemaxxing.models import OrderMode, QueueConfig


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_wal_restart_preserves_ready(tmp_path: Path):
    clock = FakeClock()
    eng = QueueEngine(
        QueueConfig(name="w", order=OrderMode.FIFO, visibility_timeout=30),
        data_dir=tmp_path,
        clock=clock,
        durable=True,
    )
    m1 = eng.enqueue("one", priority=1)
    m2 = eng.enqueue("two", priority=5)
    assert_integrity(eng)

    eng2 = QueueEngine.open(tmp_path, clock=clock)
    r = eng2.receive()
    assert r is not None
    assert r.id == m2.id
    assert r.body == "two"
    eng2.ack(r.transit_id or "")
    r2 = eng2.receive()
    assert r2 is not None
    assert r2.id == m1.id
    assert_integrity(eng2)


def test_wal_restart_preserves_delay(tmp_path: Path):
    clock = FakeClock()
    eng = QueueEngine(
        QueueConfig(name="w", order=OrderMode.FIFO, visibility_timeout=30),
        data_dir=tmp_path,
        clock=clock,
        durable=True,
    )
    eng.enqueue("later", delay=10)
    eng2 = QueueEngine.open(tmp_path, clock=clock)
    assert eng2.receive() is None
    clock.advance(10)
    r = eng2.receive()
    assert r is not None
    assert r.body == "later"


def test_wal_seq_monotonic(tmp_path: Path):
    eng = QueueEngine(
        QueueConfig(name="w", order=OrderMode.FIFO),
        data_dir=tmp_path,
        durable=True,
    )
    eng.enqueue("a")
    eng.enqueue("b")
    assert_integrity(eng)

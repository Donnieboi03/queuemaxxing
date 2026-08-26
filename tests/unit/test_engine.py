from __future__ import annotations

import pytest

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


def make_engine(order: OrderMode = OrderMode.FIFO, vt: float = 30.0, delay: float = 0.0):
    clock = FakeClock()
    eng = QueueEngine(
        QueueConfig(name="t", order=order, default_delay=delay, visibility_timeout=vt),
        clock=clock,
        durable=False,
    )
    return eng, clock


def test_fifo_same_priority():
    eng, _ = make_engine(OrderMode.FIFO)
    a = eng.enqueue("a", priority=1)
    b = eng.enqueue("b", priority=1)
    r1 = eng.receive()
    r2 = eng.receive()
    assert r1 is not None and r2 is not None
    assert r1.id == a.id
    assert r2.id == b.id
    assert_integrity(eng)


def test_lifo_same_priority():
    eng, _ = make_engine(OrderMode.LIFO)
    a = eng.enqueue("a", priority=1)
    b = eng.enqueue("b", priority=1)
    r1 = eng.receive()
    assert r1 is not None
    assert r1.id == b.id
    r2 = eng.receive()
    assert r2 is not None
    assert r2.id == a.id


def test_priority_beats_order():
    eng, _ = make_engine(OrderMode.FIFO)
    eng.enqueue("low", priority=1)
    high = eng.enqueue("high", priority=10)
    r = eng.receive()
    assert r is not None
    assert r.id == high.id


def test_delay_hides_until_available():
    eng, clock = make_engine()
    msg = eng.enqueue("later", delay=5)
    assert eng.receive() is None
    assert eng.depths()["staged"] == 1
    clock.advance(5)
    r = eng.receive()
    assert r is not None
    assert r.id == msg.id
    assert_integrity(eng)


def test_queue_default_delay():
    eng, clock = make_engine(delay=10)
    eng.enqueue("x")
    assert eng.receive() is None
    clock.advance(10)
    assert eng.receive() is not None


def test_per_message_delay_override():
    eng, clock = make_engine(delay=10)
    eng.enqueue("soon", delay=1)
    eng.enqueue("late")  # uses default 10
    clock.advance(1)
    r = eng.receive()
    assert r is not None
    assert r.body == "soon"


def test_ack_removes_message():
    eng, _ = make_engine()
    eng.enqueue("x")
    r = eng.receive()
    assert r is not None
    assert eng.ack(r.transit_id or "")
    assert eng.receive() is None
    assert eng.depths()["store"] == 0


def test_stale_ack_rejected():
    eng, _ = make_engine()
    eng.enqueue("x")
    r = eng.receive()
    assert r is not None
    assert eng.ack(r.transit_id or "")
    assert not eng.ack(r.transit_id or "")

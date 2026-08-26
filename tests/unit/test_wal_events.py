from __future__ import annotations

from queuemaxxing.models import (
    AckEvent,
    EnqueueEvent,
    event_from_dict,
    event_to_dict,
)


def test_roundtrip_enqueue_event():
    e = EnqueueEvent(seq=1, id="m1", body="hi", priority=3, order_seq=2, available_at=1.5)
    d = event_to_dict(e)
    back = event_from_dict(d)
    assert isinstance(back, EnqueueEvent)
    assert back.id == "m1"
    assert back.body == "hi"
    assert back.priority == 3


def test_roundtrip_ack_event():
    e = AckEvent(seq=2, transit_id="t1")
    back = event_from_dict(event_to_dict(e))
    assert isinstance(back, AckEvent)
    assert back.transit_id == "t1"

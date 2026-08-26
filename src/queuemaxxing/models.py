from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class OrderMode(str, Enum):
    FIFO = "fifo"
    LIFO = "lifo"


class MessageState(str, Enum):
    STAGED = "staged"
    READY = "ready"
    IN_FLIGHT = "in_flight"


@dataclass
class QueueConfig:
    name: str
    order: OrderMode = OrderMode.FIFO
    default_delay: float = 0.0
    visibility_timeout: float = 30.0


@dataclass
class Message:
    id: str
    body: str
    priority: int
    order_seq: int
    available_at: float
    state: MessageState = MessageState.READY
    transit_id: str | None = None
    visible_again_at: float | None = None
    delivery_count: int = 0


@dataclass
class QueueMetaEvent:
    type: str = "queue_meta"
    seq: int = 0
    name: str = ""
    order: str = "fifo"
    default_delay: float = 0.0
    visibility_timeout: float = 30.0


@dataclass
class EnqueueEvent:
    type: str = "enqueue"
    seq: int = 0
    id: str = ""
    body: str = ""
    priority: int = 0
    order_seq: int = 0
    available_at: float = 0.0


@dataclass
class ReceiveEvent:
    type: str = "receive"
    seq: int = 0
    id: str = ""
    transit_id: str = ""
    visible_again_at: float = 0.0


@dataclass
class AckEvent:
    type: str = "ack"
    seq: int = 0
    transit_id: str = ""


@dataclass
class ExpireEvent:
    type: str = "expire"
    seq: int = 0
    id: str = ""
    transit_id: str = ""


WalEvent = QueueMetaEvent | EnqueueEvent | ReceiveEvent | AckEvent | ExpireEvent


def event_to_dict(event: WalEvent) -> dict[str, Any]:
    d: dict[str, Any] = {"v": 1, "type": event.type, "seq": event.seq}
    if isinstance(event, QueueMetaEvent):
        d.update(
            {
                "name": event.name,
                "order": event.order,
                "default_delay": event.default_delay,
                "visibility_timeout": event.visibility_timeout,
            }
        )
    elif isinstance(event, EnqueueEvent):
        d.update(
            {
                "id": event.id,
                "body": event.body,
                "priority": event.priority,
                "order_seq": event.order_seq,
                "available_at": event.available_at,
            }
        )
    elif isinstance(event, ReceiveEvent):
        d.update(
            {
                "id": event.id,
                "transit_id": event.transit_id,
                "visible_again_at": event.visible_again_at,
            }
        )
    elif isinstance(event, AckEvent):
        d["transit_id"] = event.transit_id
    elif isinstance(event, ExpireEvent):
        d.update({"id": event.id, "transit_id": event.transit_id})
    return d


def event_from_dict(d: dict[str, Any]) -> WalEvent:
    t = d["type"]
    if t == "queue_meta":
        return QueueMetaEvent(
            seq=d["seq"],
            name=d["name"],
            order=d["order"],
            default_delay=float(d["default_delay"]),
            visibility_timeout=float(d["visibility_timeout"]),
        )
    if t == "enqueue":
        return EnqueueEvent(
            seq=d["seq"],
            id=d["id"],
            body=d["body"],
            priority=int(d["priority"]),
            order_seq=int(d["order_seq"]),
            available_at=float(d["available_at"]),
        )
    if t == "receive":
        return ReceiveEvent(
            seq=d["seq"],
            id=d["id"],
            transit_id=d["transit_id"],
            visible_again_at=float(d["visible_again_at"]),
        )
    if t == "ack":
        return AckEvent(seq=d["seq"], transit_id=d["transit_id"])
    if t == "expire":
        return ExpireEvent(seq=d["seq"], id=d["id"], transit_id=d["transit_id"])
    raise ValueError(f"unknown wal event type: {t}")

from __future__ import annotations

import heapq
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from queuemaxxing import obs
from queuemaxxing.models import (
    AckEvent,
    EnqueueEvent,
    ExpireEvent,
    Message,
    MessageState,
    OrderMode,
    QueueConfig,
    QueueMetaEvent,
    ReceiveEvent,
)
from queuemaxxing.wal import Wal

Clock = Callable[[], float]


class QueueEngine:
    """Frankenstein queue: staged (time) → ready (priority+seq) → in-flight (VT)."""

    def __init__(
        self,
        config: QueueConfig,
        *,
        data_dir: Path | None = None,
        clock: Clock | None = None,
        durable: bool = True,
        persist_meta: bool = True,
    ) -> None:
        self.config = config
        self.clock: Clock = clock or time.time
        self._lock = threading.RLock()
        self._store: dict[str, Message] = {}
        self._staged: list[tuple[float, int, str]] = []
        self._ready: list[tuple[int, int, str]] = []
        self._inflight: dict[str, str] = {}  # transit_id -> message_id
        self._order_seq = 0
        self._acked: set[str] = set()
        self._wal: Wal | None = None
        if durable and data_dir is not None:
            self._wal = Wal(data_dir / "queue.wal")
            if self._wal.path.exists() and self._wal.seq > 0:
                self._replay()
            elif persist_meta:
                self._append(
                    QueueMetaEvent(
                        name=config.name,
                        order=config.order.value,
                        default_delay=config.default_delay,
                        visibility_timeout=config.visibility_timeout,
                    )
                )
        self._refresh_metrics()

    @classmethod
    def open(cls, data_dir: Path, clock: Clock | None = None) -> QueueEngine:
        wal = Wal(data_dir / "queue.wal")
        meta: QueueMetaEvent | None = None
        for event in wal.iter_events():
            if isinstance(event, QueueMetaEvent):
                meta = event
                break
        if meta is None:
            raise FileNotFoundError(f"no queue_meta in {data_dir}")
        config = QueueConfig(
            name=meta.name,
            order=OrderMode(meta.order),
            default_delay=meta.default_delay,
            visibility_timeout=meta.visibility_timeout,
        )
        return cls(config, data_dir=data_dir, clock=clock, durable=True, persist_meta=False)

    def _append(self, event: Any) -> None:
        if self._wal is not None:
            self._wal.append(event)

    def _replay(self) -> None:
        assert self._wal is not None
        now = self.clock()
        for event in self._wal.iter_events():
            if isinstance(event, QueueMetaEvent):
                self.config = QueueConfig(
                    name=event.name,
                    order=OrderMode(event.order),
                    default_delay=event.default_delay,
                    visibility_timeout=event.visibility_timeout,
                )
            elif isinstance(event, EnqueueEvent):
                msg = Message(
                    id=event.id,
                    body=event.body,
                    priority=event.priority,
                    order_seq=event.order_seq,
                    available_at=event.available_at,
                )
                self._store[msg.id] = msg
                self._order_seq = max(self._order_seq, msg.order_seq)
                self._place_new_locked(msg, now)
            elif isinstance(event, ReceiveEvent):
                msg = self._store.get(event.id)
                if msg is None or msg.id in self._acked:
                    continue
                self._remove_from_lanes_locked(msg)
                msg.state = MessageState.IN_FLIGHT
                msg.transit_id = event.transit_id
                msg.visible_again_at = event.visible_again_at
                msg.delivery_count += 1
                self._inflight[event.transit_id] = msg.id
            elif isinstance(event, AckEvent):
                mid = self._inflight.pop(event.transit_id, None)
                if mid is None:
                    continue
                msg = self._store.pop(mid, None)
                if msg is not None:
                    self._acked.add(mid)
            elif isinstance(event, ExpireEvent):
                mid = self._inflight.pop(event.transit_id, None)
                if mid is None:
                    continue
                msg = self._store.get(mid)
                if msg is None:
                    continue
                msg.transit_id = None
                msg.visible_again_at = None
                msg.state = MessageState.READY
                self._push_ready_locked(msg)
        self._expire_and_promote_locked(now)

    def _ready_key(self, msg: Message) -> tuple[int, int, str]:
        # Higher priority first => negate priority for min-heap.
        if self.config.order == OrderMode.FIFO:
            return (-msg.priority, msg.order_seq, msg.id)
        return (-msg.priority, -msg.order_seq, msg.id)

    def _push_staged_locked(self, msg: Message) -> None:
        msg.state = MessageState.STAGED
        heapq.heappush(self._staged, (msg.available_at, msg.order_seq, msg.id))

    def _push_ready_locked(self, msg: Message) -> None:
        msg.state = MessageState.READY
        heapq.heappush(self._ready, (*self._ready_key(msg),))

    def _place_new_locked(self, msg: Message, now: float) -> None:
        if msg.available_at <= now:
            self._push_ready_locked(msg)
        else:
            self._push_staged_locked(msg)

    def _pop_staged_due_locked(self, now: float) -> Message | None:
        while self._staged:
            available_at, _, mid = self._staged[0]
            if available_at > now:
                return None
            heapq.heappop(self._staged)
            msg = self._store.get(mid)
            if msg is None or msg.state != MessageState.STAGED:
                continue
            return msg
        return None

    def _pop_ready_locked(self) -> Message | None:
        while self._ready:
            *_, mid = heapq.heappop(self._ready)
            msg = self._store.get(mid)
            if msg is None or msg.state != MessageState.READY:
                continue
            return msg
        return None

    def _remove_from_lanes_locked(self, msg: Message) -> None:
        # Lazy deletion: mark state; stale heap entries skipped on pop.
        msg.state = MessageState.READY  # temporary; caller sets final

    def _expire_and_promote_locked(self, now: float) -> list[str]:
        redelivered: list[str] = []
        expired: list[tuple[str, str]] = []
        for transit_id, mid in list(self._inflight.items()):
            msg = self._store.get(mid)
            if msg is None:
                self._inflight.pop(transit_id, None)
                continue
            if msg.visible_again_at is not None and msg.visible_again_at <= now:
                expired.append((transit_id, mid))
        for transit_id, mid in expired:
            msg = self._store.get(mid)
            if msg is None:
                continue
            self._append(ExpireEvent(id=mid, transit_id=transit_id))
            self._inflight.pop(transit_id, None)
            msg.transit_id = None
            msg.visible_again_at = None
            msg.state = MessageState.READY
            self._push_ready_locked(msg)
            redelivered.append(mid)
            obs.redeliver_total.labels(queue=self.config.name).inc()
            obs.log.info("expire", queue=self.config.name, message_id=mid, transit_id=transit_id)

        while True:
            msg = self._pop_staged_due_locked(now)
            if msg is None:
                break
            self._push_ready_locked(msg)
            obs.log.debug("promote", queue=self.config.name, message_id=msg.id)
        return redelivered

    def _refresh_metrics(self) -> None:
        name = self.config.name
        staged = sum(1 for m in self._store.values() if m.state == MessageState.STAGED)
        ready = sum(1 for m in self._store.values() if m.state == MessageState.READY)
        inflight = sum(1 for m in self._store.values() if m.state == MessageState.IN_FLIGHT)
        wal_seq = self._wal.seq if self._wal else 0
        obs.update_depth_gauges(name, staged, ready, inflight, wal_seq)

    def enqueue(
        self,
        body: str,
        *,
        priority: int = 0,
        delay: float | None = None,
        message_id: str | None = None,
    ) -> Message:
        with self._lock:
            now = self.clock()
            self._expire_and_promote_locked(now)
            effective_delay = self.config.default_delay if delay is None else delay
            if effective_delay < 0:
                raise ValueError("delay must be >= 0")
            self._order_seq += 1
            msg = Message(
                id=message_id or str(uuid.uuid4()),
                body=body,
                priority=priority,
                order_seq=self._order_seq,
                available_at=now + effective_delay,
            )
            self._append(
                EnqueueEvent(
                    id=msg.id,
                    body=msg.body,
                    priority=msg.priority,
                    order_seq=msg.order_seq,
                    available_at=msg.available_at,
                )
            )
            self._store[msg.id] = msg
            self._place_new_locked(msg, now)
            obs.enqueue_total.labels(queue=self.config.name).inc()
            obs.log.info(
                "enqueue",
                queue=self.config.name,
                message_id=msg.id,
                priority=msg.priority,
                available_at=msg.available_at,
            )
            self._refresh_metrics()
            return msg

    def receive(self) -> Message | None:
        with self._lock:
            now = self.clock()
            self._expire_and_promote_locked(now)
            msg = self._pop_ready_locked()
            if msg is None:
                self._refresh_metrics()
                return None
            transit_id = str(uuid.uuid4())
            visible_again = now + self.config.visibility_timeout
            self._append(
                ReceiveEvent(id=msg.id, transit_id=transit_id, visible_again_at=visible_again)
            )
            msg.state = MessageState.IN_FLIGHT
            msg.transit_id = transit_id
            msg.visible_again_at = visible_again
            msg.delivery_count += 1
            self._inflight[transit_id] = msg.id
            obs.receive_total.labels(queue=self.config.name).inc()
            obs.log.info(
                "receive",
                queue=self.config.name,
                message_id=msg.id,
                transit_id=transit_id,
            )
            self._refresh_metrics()
            return msg

    def ack(self, transit_id: str) -> bool:
        with self._lock:
            mid = self._inflight.get(transit_id)
            if mid is None:
                obs.log.warning("ack_stale", queue=self.config.name, transit_id=transit_id)
                return False
            self._append(AckEvent(transit_id=transit_id))
            self._inflight.pop(transit_id, None)
            self._store.pop(mid, None)
            self._acked.add(mid)
            obs.ack_total.labels(queue=self.config.name).inc()
            obs.log.info("ack", queue=self.config.name, message_id=mid, transit_id=transit_id)
            self._refresh_metrics()
            return True

    def tick(self) -> list[str]:
        """Run expire+promote (sweeper / tests)."""
        with self._lock:
            out = self._expire_and_promote_locked(self.clock())
            self._refresh_metrics()
            return out

    def depths(self) -> dict[str, int]:
        with self._lock:
            return {
                "staged": sum(1 for m in self._store.values() if m.state == MessageState.STAGED),
                "ready": sum(1 for m in self._store.values() if m.state == MessageState.READY),
                "in_flight": sum(
                    1 for m in self._store.values() if m.state == MessageState.IN_FLIGHT
                ),
                "store": len(self._store),
            }

    def snapshot_for_integrity(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.config.name,
                "store": dict(self._store),
                "staged_ids": [t[2] for t in self._staged],
                "ready_ids": [t[2] for t in self._ready],
                "inflight": dict(self._inflight),
                "acked": set(self._acked),
                "wal_path": str(self._wal.path) if self._wal else None,
                "wal_seq": self._wal.seq if self._wal else 0,
            }

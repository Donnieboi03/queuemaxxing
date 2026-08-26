from __future__ import annotations

import threading
from typing import Any

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

log = structlog.get_logger("queuemaxxing")

_registry = CollectorRegistry()

enqueue_total = Counter(
    "queuemaxxing_enqueue_total",
    "Messages enqueued",
    ["queue"],
    registry=_registry,
)
receive_total = Counter(
    "queuemaxxing_receive_total",
    "Messages received",
    ["queue"],
    registry=_registry,
)
ack_total = Counter(
    "queuemaxxing_ack_total",
    "Messages acked",
    ["queue"],
    registry=_registry,
)
redeliver_total = Counter(
    "queuemaxxing_redeliver_total",
    "Visibility-timeout redeliveries",
    ["queue"],
    registry=_registry,
)
integrity_failures = Counter(
    "queuemaxxing_integrity_failures_total",
    "Integrity auditor failures",
    ["queue"],
    registry=_registry,
)
staged_depth = Gauge(
    "queuemaxxing_staged_depth",
    "Staged lane depth",
    ["queue"],
    registry=_registry,
)
ready_depth = Gauge(
    "queuemaxxing_ready_depth",
    "Ready lane depth",
    ["queue"],
    registry=_registry,
)
inflight_depth = Gauge(
    "queuemaxxing_inflight_depth",
    "In-flight depth",
    ["queue"],
    registry=_registry,
)
wal_seq_gauge = Gauge(
    "queuemaxxing_wal_seq",
    "Last WAL sequence number",
    ["queue"],
    registry=_registry,
)

_metrics_lock = threading.Lock()


def metrics_text() -> bytes:
    return generate_latest(_registry)


def configure_logging(json_logs: bool = False) -> None:
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
    ]
    if json_logs:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def update_depth_gauges(queue: str, staged: int, ready: int, inflight: int, wal_seq: int) -> None:
    staged_depth.labels(queue=queue).set(staged)
    ready_depth.labels(queue=queue).set(ready)
    inflight_depth.labels(queue=queue).set(inflight)
    wal_seq_gauge.labels(queue=queue).set(wal_seq)

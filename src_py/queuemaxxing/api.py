from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel, Field

from queuemaxxing import obs
from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import audit_snapshot
from queuemaxxing.models import OrderMode, QueueConfig
from queuemaxxing.sweeper import VisibilitySweeper

DATA_ROOT = Path(os.environ.get("QUEUEMAXXING_DATA", "./data"))


class CreateQueueRequest(BaseModel):
    name: str
    order: OrderMode = OrderMode.FIFO
    default_delay: float = Field(0.0, ge=0)
    visibility_timeout: float = Field(30.0, gt=0)


class EnqueueRequest(BaseModel):
    body: str
    priority: int = 0
    delay: float | None = Field(default=None, ge=0)


class AckRequest(BaseModel):
    transit_id: str


class Registry:
    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root
        self.data_root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._engines: dict[str, QueueEngine] = {}
        self.sweeper = VisibilitySweeper(interval=0.1)

    def start(self) -> None:
        if self.data_root.exists():
            for path in self.data_root.iterdir():
                if path.is_dir() and (path / "queue.wal").exists():
                    try:
                        engine = QueueEngine.open(path)
                        self._engines[engine.config.name] = engine
                        self.sweeper.add(engine)
                    except Exception:
                        continue
        self.sweeper.start()

    def stop(self) -> None:
        self.sweeper.stop()

    def create(self, req: CreateQueueRequest) -> QueueEngine:
        with self._lock:
            if req.name in self._engines:
                raise HTTPException(status_code=409, detail="queue already exists")
            qdir = self.data_root / req.name
            config = QueueConfig(
                name=req.name,
                order=req.order,
                default_delay=req.default_delay,
                visibility_timeout=req.visibility_timeout,
            )
            engine = QueueEngine(config, data_dir=qdir, durable=True)
            self._engines[req.name] = engine
            self.sweeper.add(engine)
            return engine

    def get(self, name: str) -> QueueEngine:
        with self._lock:
            engine = self._engines.get(name)
            if engine is None:
                raise HTTPException(status_code=404, detail="queue not found")
            return engine

    def list_names(self) -> list[str]:
        with self._lock:
            return sorted(self._engines)


def create_app(data_root: Path | None = None) -> FastAPI:
    root = Path(data_root) if data_root is not None else Path(
        os.environ.get("QUEUEMAXXING_DATA", "./data")
    )
    registry = Registry(root)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        obs.configure_logging()
        registry.start()
        yield
        registry.stop()

    app = FastAPI(title="Queuemaxxing", version="0.1.0", lifespan=lifespan)
    app.state.registry = registry

    @app.get("/health")
    def health() -> dict:
        return {"ok": True, "queues": registry.list_names()}

    @app.get("/metrics")
    def metrics() -> Response:
        return Response(content=obs.metrics_text(), media_type="text/plain; version=0.0.4")

    @app.post("/queues")
    def create_queue(req: CreateQueueRequest) -> dict:
        engine = registry.create(req)
        return {
            "name": engine.config.name,
            "order": engine.config.order.value,
            "default_delay": engine.config.default_delay,
            "visibility_timeout": engine.config.visibility_timeout,
        }

    @app.get("/queues")
    def list_queues() -> dict:
        return {"queues": registry.list_names()}

    @app.post("/queues/{name}/messages")
    def enqueue(name: str, req: EnqueueRequest) -> dict:
        engine = registry.get(name)
        msg = engine.enqueue(req.body, priority=req.priority, delay=req.delay)
        return {
            "message_id": msg.id,
            "priority": msg.priority,
            "order_seq": msg.order_seq,
            "available_at": msg.available_at,
            "state": msg.state.value,
        }

    @app.post("/queues/{name}/receive", response_model=None)
    def receive(
        name: str,
        wait_seconds: float = Query(0.0, ge=0, le=30),
    ):
        engine = registry.get(name)
        deadline = time.time() + wait_seconds
        while True:
            msg = engine.receive()
            if msg is not None:
                return {
                    "message_id": msg.id,
                    "transit_id": msg.transit_id,
                    "body": msg.body,
                    "priority": msg.priority,
                    "delivery_count": msg.delivery_count,
                    "visible_again_at": msg.visible_again_at,
                }
            if time.time() >= deadline:
                return Response(status_code=204)
            time.sleep(0.05)

    @app.post("/queues/{name}/ack")
    def ack(name: str, req: AckRequest) -> dict:
        engine = registry.get(name)
        ok = engine.ack(req.transit_id)
        if not ok:
            raise HTTPException(status_code=404, detail="unknown or stale transit_id")
        return {"acked": True, "transit_id": req.transit_id}

    @app.get("/queues/{name}/depths")
    def depths(name: str) -> dict:
        return registry.get(name).depths()

    @app.get("/debug/integrity")
    def debug_integrity(name: str | None = None) -> dict:
        names = [name] if name else registry.list_names()
        report = {}
        for n in names:
            engine = registry.get(n)
            failures = audit_snapshot(engine.snapshot_for_integrity())
            report[n] = {"ok": not failures, "failures": failures}
        return report

    return app


app = create_app()

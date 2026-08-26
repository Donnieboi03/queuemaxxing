from __future__ import annotations

import threading
import time
from typing import Iterable

from queuemaxxing.engine import QueueEngine


class VisibilitySweeper:
    """Background VT expiry (+ promote) across engines."""

    def __init__(self, engines: Iterable[QueueEngine] | None = None, interval: float = 0.2) -> None:
        self._engines: list[QueueEngine] = list(engines or [])
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def add(self, engine: QueueEngine) -> None:
        with self._lock:
            self._engines.append(engine)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="queuemaxxing-sweeper", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            with self._lock:
                engines = list(self._engines)
            for engine in engines:
                try:
                    engine.tick()
                except Exception:
                    # Keep sweeper alive; ops layer logs elsewhere.
                    continue
            time.sleep(0)  # yield

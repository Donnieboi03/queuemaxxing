from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterator

from queuemaxxing.models import WalEvent, event_from_dict, event_to_dict


class Wal:
    """Append-only JSONL write-ahead log. Append+fsync before callers mutate RAM."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._seq = 0
        if self.path.exists():
            for event in self.iter_events():
                self._seq = max(self._seq, event.seq)

    @property
    def seq(self) -> int:
        return self._seq

    def append(self, event: WalEvent) -> WalEvent:
        self._seq += 1
        event.seq = self._seq
        line = json.dumps(event_to_dict(event), separators=(",", ":")) + "\n"
        with self.path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
        return event

    def iter_events(self) -> Iterator[WalEvent]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield event_from_dict(json.loads(line))

    def last_seq_on_disk(self) -> int:
        last = 0
        prev = 0
        for event in self.iter_events():
            if event.seq <= prev:
                raise ValueError(f"WAL seq not monotonic: {prev} -> {event.seq}")
            prev = event.seq
            last = event.seq
        return last

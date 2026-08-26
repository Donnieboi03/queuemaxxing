from __future__ import annotations

from pathlib import Path
from typing import Any

from queuemaxxing import obs
from queuemaxxing.models import MessageState
from queuemaxxing.wal import Wal


class IntegrityError(Exception):
    def __init__(self, failures: list[str]) -> None:
        self.failures = failures
        super().__init__("; ".join(failures))


def audit_snapshot(snapshot: dict[str, Any]) -> list[str]:
    """Return list of integrity failure strings (empty => ok)."""
    failures: list[str] = []
    store: dict = snapshot["store"]
    inflight: dict = snapshot["inflight"]
    acked: set = snapshot["acked"]
    name = snapshot.get("name", "")

    for mid, msg in store.items():
        if mid in acked:
            failures.append(f"message {mid} in store and acked")
        if msg.state == MessageState.IN_FLIGHT:
            if msg.transit_id is None or inflight.get(msg.transit_id) != mid:
                failures.append(f"in_flight message {mid} missing/mismatched transit")
        elif msg.transit_id is not None:
            failures.append(f"non-in_flight message {mid} has transit_id")

    seen_transit: set[str] = set()
    for transit_id, mid in inflight.items():
        if transit_id in seen_transit:
            failures.append(f"duplicate transit {transit_id}")
        seen_transit.add(transit_id)
        msg = store.get(mid)
        if msg is None:
            failures.append(f"inflight {transit_id} -> missing {mid}")
        elif msg.state != MessageState.IN_FLIGHT:
            failures.append(f"inflight {transit_id} -> {mid} state={msg.state}")
        elif msg.transit_id != transit_id:
            failures.append(f"inflight transit mismatch for {mid}")

    for mid in snapshot["staged_ids"]:
        if mid not in store and mid not in acked:
            failures.append(f"staged heap unknown id {mid}")
    for mid in snapshot["ready_ids"]:
        if mid not in store and mid not in acked:
            failures.append(f"ready heap unknown id {mid}")

    staged = {m.id for m in store.values() if m.state == MessageState.STAGED}
    ready = {m.id for m in store.values() if m.state == MessageState.READY}
    inflight_ids = {m.id for m in store.values() if m.state == MessageState.IN_FLIGHT}
    if staged & ready:
        failures.append(f"staged∩ready nonempty: {staged & ready}")
    if staged & inflight_ids:
        failures.append(f"staged∩inflight nonempty: {staged & inflight_ids}")
    if ready & inflight_ids:
        failures.append(f"ready∩inflight nonempty: {ready & inflight_ids}")

    wal_path = snapshot.get("wal_path")
    if wal_path:
        try:
            disk_seq = Wal(Path(wal_path)).last_seq_on_disk()
            mem_seq = int(snapshot.get("wal_seq") or 0)
            if disk_seq != mem_seq:
                failures.append(f"wal seq mismatch disk={disk_seq} mem={mem_seq}")
        except ValueError as e:
            failures.append(str(e))
        except OSError as e:
            failures.append(f"wal read error: {e}")

    if failures and name:
        obs.integrity_failures.labels(queue=name).inc(len(failures))
    return failures


def assert_integrity(engine: Any) -> None:
    failures = audit_snapshot(engine.snapshot_for_integrity())
    if failures:
        raise IntegrityError(failures)

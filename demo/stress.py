#!/usr/bin/env python3
"""Throughput stress: engine (mem/WAL) and HTTP MPMC rates."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

# Allow running without install: python demo/stress.py ...
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fastapi.testclient import TestClient

from queuemaxxing.api import create_app
from queuemaxxing.engine import QueueEngine
from queuemaxxing.integrity import assert_integrity
from queuemaxxing.models import OrderMode, QueueConfig


def quiet_logs() -> None:
    logging.disable(logging.CRITICAL)
    try:
        import structlog

        structlog.configure(
            processors=[structlog.processors.TimeStamper(fmt="iso")],
            wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL),
            cache_logger_on_first_use=False,
        )
    except Exception:
        pass
    # eng/obs may already have a bound logger — mute root handlers too
    for name in ("queuemaxxing", "uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False


def repo_tmp() -> Path:
    p = ROOT / "tmp"
    p.mkdir(parents=True, exist_ok=True)
    (p / "data").mkdir(parents=True, exist_ok=True)
    return p


def write_report(payload: dict) -> Path:
    tmp = repo_tmp()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = tmp / f"stress-{ts}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def run_engine(args: argparse.Namespace) -> dict:
    quiet_logs()
    n = args.messages
    n_prod = args.producers
    n_cons = args.consumers
    per = n // n_prod
    rem = n % n_prod

    data_dir = None
    durable = False
    if args.wal:
        durable = True
        data_dir = repo_tmp() / "data" / f"stress-engine-{int(time.time())}"
        if data_dir.exists():
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True)

    eng = QueueEngine(
        QueueConfig(
            name="stress",
            order=OrderMode.FIFO,
            default_delay=0.0,
            visibility_timeout=args.vt,
        ),
        data_dir=data_dir,
        durable=durable,
        persist_meta=durable,
    )

    errors: list[str] = []

    def produce(k: int, count: int) -> None:
        try:
            for i in range(count):
                eng.enqueue(f"p{k}-{i}", priority=i % 16)
        except Exception as e:
            errors.append(f"produce {k}: {e}")

    t0 = time.perf_counter()
    threads = []
    for k in range(n_prod):
        count = per + (rem if k == n_prod - 1 else 0)
        t = threading.Thread(target=produce, args=(k, count))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    t1 = time.perf_counter()

    got: list[str] = []
    lock = threading.Lock()

    def consume() -> None:
        try:
            while True:
                with lock:
                    if len(got) >= n:
                        return
                msg = eng.receive()
                if msg is None:
                    time.sleep(0.0005)
                    continue
                if not eng.ack(msg.transit_id or ""):
                    errors.append("ack failed")
                    return
                with lock:
                    got.append(msg.id)
        except Exception as e:
            errors.append(f"consume: {e}")

    cons = [threading.Thread(target=consume) for _ in range(n_cons)]
    for t in cons:
        t.start()
    for t in cons:
        t.join(timeout=120)
    t2 = time.perf_counter()

    assert_integrity(eng)
    enq_s = t1 - t0
    out_s = t2 - t1
    report = {
        "mode": "engine",
        "wal": bool(args.wal),
        "messages": n,
        "producers": n_prod,
        "consumers": n_cons,
        "acked": len(got),
        "unique": len(set(got)),
        "enqueue_seconds": enq_s,
        "consume_seconds": out_s,
        "enqueue_msg_per_s": (n / enq_s) if enq_s > 0 else 0,
        "consume_msg_per_s": (len(got) / out_s) if out_s > 0 else 0,
        "errors": errors,
        "depths": eng.depths(),
    }
    return report


def run_http(args: argparse.Namespace) -> dict:
    quiet_logs()
    n = args.messages
    n_prod = args.producers
    n_cons = args.consumers
    per = n // n_prod
    rem = n % n_prod

    data_root = repo_tmp() / "data" / f"stress-http-{int(time.time())}"
    if data_root.exists():
        shutil.rmtree(data_root)
    data_root.mkdir(parents=True)

    app = create_app(data_root=data_root)
    errors: list[str] = []
    qname = "stress"

    with TestClient(app) as client:
        r = client.post(
            "/queues",
            json={
                "name": qname,
                "order": "fifo",
                "default_delay": 0,
                "visibility_timeout": args.vt,
            },
        )
        assert r.status_code == 200, r.text

        def produce(k: int, count: int) -> None:
            try:
                with TestClient(app) as c:
                    for i in range(count):
                        rr = c.post(
                            f"/queues/{qname}/messages",
                            json={"body": f"p{k}-{i}", "priority": i % 16},
                        )
                        if rr.status_code != 200:
                            errors.append(f"enqueue {rr.status_code}")
                            return
            except Exception as e:
                errors.append(f"produce {k}: {e}")

        t0 = time.perf_counter()
        threads = []
        for k in range(n_prod):
            count = per + (rem if k == n_prod - 1 else 0)
            t = threading.Thread(target=produce, args=(k, count))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        t1 = time.perf_counter()

        got: list[str] = []
        lock = threading.Lock()

        def consume() -> None:
            try:
                with TestClient(app) as c:
                    while True:
                        with lock:
                            if len(got) >= n:
                                return
                        rr = c.post(f"/queues/{qname}/receive")
                        if rr.status_code == 204:
                            time.sleep(0.001)
                            continue
                        if rr.status_code != 200:
                            errors.append(f"receive {rr.status_code}")
                            return
                        msg = rr.json()
                        ar = c.post(
                            f"/queues/{qname}/ack",
                            json={"transit_id": msg["transit_id"]},
                        )
                        if ar.status_code != 200:
                            errors.append(f"ack {ar.status_code}")
                            return
                        with lock:
                            got.append(msg["message_id"])
            except Exception as e:
                errors.append(f"consume: {e}")

        cons = [threading.Thread(target=consume) for _ in range(n_cons)]
        for t in cons:
            t.start()
        for t in cons:
            t.join(timeout=180)
        t2 = time.perf_counter()

        integ = client.get("/debug/integrity", params={"name": qname})
        integrity_ok = integ.status_code == 200 and integ.json().get(qname, {}).get("ok")

    enq_s = t1 - t0
    out_s = t2 - t1
    return {
        "mode": "http",
        "messages": n,
        "producers": n_prod,
        "consumers": n_cons,
        "acked": len(got),
        "unique": len(set(got)),
        "enqueue_seconds": enq_s,
        "consume_seconds": out_s,
        "enqueue_msg_per_s": (n / enq_s) if enq_s > 0 else 0,
        "consume_msg_per_s": (len(got) / out_s) if out_s > 0 else 0,
        "errors": errors,
        "integrity_ok": integrity_ok,
        "data_root": str(data_root),
    }


def print_report(report: dict) -> None:
    print("--- stress summary ---")
    print(f"mode:            {report['mode']}")
    if "wal" in report:
        print(f"wal:             {report['wal']}")
    print(f"messages:        {report['messages']}")
    print(f"producers/cons:  {report['producers']}/{report['consumers']}")
    print(f"acked/unique:    {report['acked']}/{report['unique']}")
    print(f"enqueue msg/s:   {report['enqueue_msg_per_s']:.1f}")
    print(f"consume msg/s:   {report['consume_msg_per_s']:.1f}")
    print(f"enqueue sec:     {report['enqueue_seconds']:.4f}")
    print(f"consume sec:     {report['consume_seconds']:.4f}")
    if report.get("errors"):
        print(f"errors:          {report['errors']}")


def main() -> None:
    p = argparse.ArgumentParser(description="Queuemaxxing throughput stress")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--messages", type=int, default=5000)
        sp.add_argument("--producers", type=int, default=4)
        sp.add_argument("--consumers", type=int, default=4)
        sp.add_argument("--vt", type=float, default=60.0)

    pe = sub.add_parser("engine", help="Direct QueueEngine bench")
    add_common(pe)
    pe.add_argument("--wal", action="store_true", help="Durable JSONL WAL under tmp/data")

    ph = sub.add_parser("http", help="HTTP API bench via TestClient")
    add_common(ph)

    args = p.parse_args()
    if args.cmd == "engine":
        report = run_engine(args)
    else:
        report = run_http(args)

    path = write_report(report)
    print_report(report)
    print(f"report: {path}")
    if report.get("errors") or report["acked"] != report["messages"]:
        raise SystemExit(1)
    if report["unique"] != report["messages"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

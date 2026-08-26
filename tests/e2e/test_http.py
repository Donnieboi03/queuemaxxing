from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from queuemaxxing.api import create_app


@pytest.fixture
def client(tmp_path: Path):
    app = create_app(data_root=tmp_path)
    with TestClient(app) as c:
        yield c, tmp_path


def test_http_flow(client):
    c, _ = client
    assert (
        c.post(
            "/queues",
            json={"name": "q1", "order": "lifo", "visibility_timeout": 30},
        ).status_code
        == 200
    )
    c.post("/queues/q1/messages", json={"body": "a", "priority": 1})
    c.post("/queues/q1/messages", json={"body": "b", "priority": 1})
    r1 = c.post("/queues/q1/receive")
    assert r1.status_code == 200
    assert r1.json()["body"] == "b"  # LIFO
    assert c.post("/queues/q1/ack", json={"transit_id": r1.json()["transit_id"]}).status_code == 200
    r2 = c.post("/queues/q1/receive")
    assert r2.status_code == 200
    assert r2.json()["body"] == "a"
    assert c.get("/health").status_code == 200
    assert c.get("/metrics").status_code == 200
    integ = c.get("/debug/integrity")
    assert integ.status_code == 200
    assert integ.json()["q1"]["ok"] is True


def test_http_empty_receive(client):
    c, _ = client
    c.post("/queues", json={"name": "empty", "visibility_timeout": 5})
    r = c.post("/queues/empty/receive")
    assert r.status_code == 204


def test_http_priority_and_delay(client):
    c, _ = client
    c.post("/queues", json={"name": "pd", "order": "fifo", "visibility_timeout": 30})
    c.post("/queues/pd/messages", json={"body": "low", "priority": 1})
    c.post("/queues/pd/messages", json={"body": "high", "priority": 9})
    r = c.post("/queues/pd/receive")
    assert r.json()["body"] == "high"


def test_http_multi_client_mpmc(client):
    c, _ = client
    c.post("/queues", json={"name": "m", "order": "fifo", "visibility_timeout": 60})
    for i in range(20):
        assert c.post("/queues/m/messages", json={"body": str(i), "priority": i % 3}).status_code == 200
    bodies = []
    for _ in range(20):
        r = c.post("/queues/m/receive")
        assert r.status_code == 200
        bodies.append(r.json()["body"])
        assert c.post("/queues/m/ack", json={"transit_id": r.json()["transit_id"]}).status_code == 200
    assert len(bodies) == 20
    assert c.post("/queues/m/receive").status_code == 204


def test_restart_via_new_app(client):
    c, tmp = client
    c.post("/queues", json={"name": "persist", "visibility_timeout": 30})
    c.post("/queues/persist/messages", json={"body": "survives"})
    app2 = create_app(data_root=tmp)
    with TestClient(app2) as c2:
        r = c2.post("/queues/persist/receive")
        assert r.status_code == 200
        assert r.json()["body"] == "survives"

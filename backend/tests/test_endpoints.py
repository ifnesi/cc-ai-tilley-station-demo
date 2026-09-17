"""Phase-3 endpoint stop gate: demo control, validation, static serving."""

from __future__ import annotations

import fastavro
import pytest

from backend.app import create_app
from backend.config import FRONTEND_DIR
from emulator.schemas import load_schema

DEMO_SCHEMA = fastavro.parse_schema(load_schema("demo_control"))


# --- /demo/surge + /demo/reset produce well-formed records -----------------

def test_surge_no_body_uses_defaults(client, fake_producer):
    resp = client.post("/demo/surge")
    assert resp.status_code == 202
    body = resp.get_json()
    assert body["status"] == "accepted"
    assert body["command"] == "SURGE"
    assert len(fake_producer.sent) == 1
    record = fake_producer.sent[0]
    assert record["command"] == "SURGE"
    assert record["factor"] is None
    assert record["duration_seconds"] is None
    # well-formed against demo_control.avsc
    assert fastavro.validation.validate(record, DEMO_SCHEMA, raise_errors=True)


def test_surge_with_overrides(client, fake_producer):
    resp = client.post("/demo/surge", json={"factor": 8, "duration_seconds": 30})
    assert resp.status_code == 202
    record = fake_producer.sent[0]
    assert record["factor"] == 8.0
    assert record["duration_seconds"] == 30
    assert fastavro.validation.validate(record, DEMO_SCHEMA, raise_errors=True)


def test_reset_produces_reset_record(client, fake_producer):
    resp = client.post("/demo/reset")
    assert resp.status_code == 202
    assert resp.get_json()["command"] == "RESET"
    record = fake_producer.sent[0]
    assert record["command"] == "RESET"
    assert fastavro.validation.validate(record, DEMO_SCHEMA, raise_errors=True)


@pytest.mark.parametrize(
    "bad_body",
    [
        {"factor": -1},
        {"factor": 0},
        {"factor": "not-a-number"},
        {"duration_seconds": -5},
        {"duration_seconds": 2.5},
        {"duration_seconds": "abc"},
    ],
)
def test_bad_input_returns_400(client, fake_producer, bad_body):
    resp = client.post("/demo/surge", json=bad_body)
    assert resp.status_code == 400
    assert "error" in resp.get_json()
    assert fake_producer.sent == []  # nothing produced on invalid input


def test_non_object_json_body_returns_400(client):
    resp = client.post("/demo/surge", json=[1, 2, 3])
    assert resp.status_code == 400


def test_producer_unavailable_returns_503():
    app, _ = create_app(demo_producer=None, async_mode="threading", frontend_dir=FRONTEND_DIR)
    resp = app.test_client().post("/demo/surge")
    assert resp.status_code == 503


# --- health + static serving ----------------------------------------------

def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "ok"


def test_index_served(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Tilley" in resp.data  # placeholder or real frontend index.html


def test_static_file_served(client):
    # styles.css ships with the frontend skeleton.
    resp = client.get("/styles.css")
    assert resp.status_code == 200

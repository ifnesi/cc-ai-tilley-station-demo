"""Phase-3 unit stop gate: deserialised record -> correct channel + payload."""

from __future__ import annotations

import datetime as dt

import pytest

from backend.consumers import (
    CONSUMED_TOPICS,
    RAW_EVENT_TOPICS,
    TOPIC_CHANNELS,
    build_message,
    emit_record,
    to_wire,
)
from backend.tests.conftest import RecordingSocketIO
from emulator.samples import sample_record

EXPECTED_CHANNELS = {
    "train_in_transit": "raw_event",
    "train_in_station": "raw_event",
    "passengers_flow": "raw_event",
    "station_occupancy": "occupancy",
    "station_metrics": "metrics",
    "station_anomalies": "anomaly",
    "signal_state": "signal",
    "gateline_state": "gateline",
    "station_ai_suggestions": "ai_suggestion",
    "operator_answers": "operator_answer",
}


def test_channel_map_matches_spec():
    assert TOPIC_CHANNELS == EXPECTED_CHANNELS


def _assert_json_safe(value):
    """No datetimes survive to the wire; only JSON-native scalars/containers."""
    if isinstance(value, dict):
        for v in value.values():
            _assert_json_safe(v)
    elif isinstance(value, list):
        for v in value:
            _assert_json_safe(v)
    else:
        assert not isinstance(value, dt.datetime)
        assert isinstance(value, (str, int, float, bool, type(None)))


@pytest.mark.parametrize("topic", CONSUMED_TOPICS)
def test_emit_record_channel_and_shape(topic):
    sio = RecordingSocketIO()
    record = sample_record(topic)  # same shape AvroDeserializer produces

    channel, payload = emit_record(sio, topic, record)

    assert channel == EXPECTED_CHANNELS[topic]
    assert sio.emits == [(channel, payload)]
    _assert_json_safe(payload)


@pytest.mark.parametrize("topic", sorted(RAW_EVENT_TOPICS))
def test_raw_event_wrapping(topic):
    channel, payload = build_message(topic, sample_record(topic))
    assert channel == "raw_event"
    assert payload["event_type"] == topic
    assert isinstance(payload["data"], dict)
    # event_time carried through as epoch millis.
    assert isinstance(payload["data"]["event_time"], int)


@pytest.mark.parametrize("topic", ["station_occupancy", "station_metrics", "signal_state"])
def test_non_raw_payload_is_the_record(topic):
    channel, payload = build_message(topic, sample_record(topic))
    assert channel != "raw_event"
    assert "event_type" not in payload  # emitted directly, not wrapped


def test_to_wire_converts_datetime_to_epoch_millis():
    d = dt.datetime(2026, 9, 16, 12, 0, 0, tzinfo=dt.timezone.utc)
    assert to_wire(d) == int(d.timestamp() * 1000)


def test_to_wire_recurses_and_preserves_scalars():
    d = dt.datetime(2026, 9, 16, 12, 0, 0, tzinfo=dt.timezone.utc)
    out = to_wire({"a": d, "b": [d, 1, "x"], "c": {"d": d}, "e": True, "f": None})
    ms = int(d.timestamp() * 1000)
    assert out == {"a": ms, "b": [ms, 1, "x"], "c": {"d": ms}, "e": True, "f": None}


def test_signal_and_gateline_enums_pass_through_as_strings():
    _, payload = build_message("signal_state", sample_record("signal_state"))
    assert payload["side"] in ("LEFT", "RIGHT")
    assert payload["state"] in ("RED", "GREEN")
    _, gpayload = build_message("gateline_state", sample_record("gateline_state"))
    assert gpayload["state"] in ("OPEN", "RESTRICTED", "CLOSED")

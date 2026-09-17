"""Phase-0 stop gate.

- Every .avsc compiles.
- A sample record for each topic serializes and deserializes cleanly
  (Avro round-trip) and validates against its schema.
- reference.py exposes the domain constants + four train types.
- .env_example lists every key in
"""

from __future__ import annotations

import datetime as dt
import io
import pathlib

import fastavro
import pytest

from emulator import reference as ref
from emulator import samples
from emulator.schemas import ALL_TOPICS, DERIVED_TOPICS, RAW_TOPICS, load_schema

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


# --- schema compilation + round-trip --------------------------------------

def test_topic_inventory_matches_spec():
    #: 5 raw + 5 derived topics.
    assert set(RAW_TOPICS) == {
        "train_in_transit",
        "train_in_station",
        "passengers_flow",
        "station_occupancy",
        "demo_control",
        "operator_questions",
    }
    assert set(DERIVED_TOPICS) == {
        "station_metrics",
        "station_anomalies",
        "signal_state",
        "gateline_state",
        "station_ai_suggestions",
        "operator_answers",
    }


@pytest.mark.parametrize("topic", ALL_TOPICS)
def test_avsc_compiles(topic):
    schema = load_schema(topic)
    parsed = fastavro.parse_schema(schema)  # raises if malformed
    assert parsed is not None
    # Namespaced record name mirrors the topic name.
    assert schema["name"] == topic
    assert schema["namespace"] == "tilley"


@pytest.mark.parametrize("topic", ALL_TOPICS)
def test_sample_record_validates(topic):
    schema = fastavro.parse_schema(load_schema(topic))
    record = samples.sample_record(topic)
    assert fastavro.validation.validate(record, schema, raise_errors=True)


@pytest.mark.parametrize("topic", ALL_TOPICS)
def test_avro_round_trip(topic):
    schema = fastavro.parse_schema(load_schema(topic))
    record = samples.sample_record(topic)

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)

    for key, original in record.items():
        got = decoded[key]
        if isinstance(original, dt.datetime):
            # timestamp-millis round-trips as an aware UTC datetime, equal instant.
            assert isinstance(got, dt.datetime)
            assert got.timestamp() == pytest.approx(original.timestamp(), abs=1e-3)
        elif isinstance(original, float):
            assert got == pytest.approx(original)
        else:
            assert got == original


def test_optional_passengers_boarding_defaults_null():
    # train_in_transit.passengers_boarding is a nullable union defaulting to null,
    # so an approach record without it must still encode/decode.
    schema = fastavro.parse_schema(load_schema("train_in_transit"))
    record = samples.sample_record("train_in_transit")
    record.pop("passengers_boarding")  # rely on the schema default
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, schema)
    assert decoded["passengers_boarding"] is None


# --- reference.py matches ----------------------------------------------

def test_station_reference_matches_spec():
    assert ref.STATION_DATA["station_name"] == ref.STATION_NAME
    assert ref.STATION_DATA["capacity"] == ref.STATION_CAPACITY


def test_train_reference_matches_spec():
    assert ref.TRAIN_DATA == {
        "A72": {"capacity": 200},
        "B85": {"capacity": 225},
        "C98": {"capacity": 250},
        "D11": {"capacity": 275},
    }
    assert ref.train_capacity("D11") == 275


def test_direction_convention():
    #: east_bound enters via LEFT and departs to Jones (east);
    # west_bound enters via RIGHT and departs to Overton (west).
    assert ref.entry_side(ref.EAST_BOUND) == "LEFT"
    assert ref.departs_to(ref.EAST_BOUND) == "Jones Station"
    assert ref.arrives_from(ref.EAST_BOUND) == "Overton Station"
    assert ref.entry_side(ref.WEST_BOUND) == "RIGHT"
    assert ref.departs_to(ref.WEST_BOUND) == "Overton Station"
    assert ref.arrives_from(ref.WEST_BOUND) == "Jones Station"


# --- .env_example lists every key -------------------------------------

SPEC_ENV_KEYS = {
    "CONFLUENT_CLOUD_API_KEY",
    "CONFLUENT_CLOUD_API_SECRET",
    "TF_VAR_aws_access_key",
    "TF_VAR_aws_secret_key",
    "BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
    "CONSUMER_GROUP",
    "FOOT_MIN",
    "FOOT_MAX",
    "TRAIN_MIN",
    "TRAIN_MAX",
    "DWELL_MIN",
    "DWELL_MAX",
    "BOARD_FRACTION",
    "SURGE_FACTOR",
    "SURGE_DURATION",
    "AUTO_SURGE_AFTER",
    "INITIAL_OCCUPANCY_FRACTION",
    # Domain constants now live in .env (no separate demo.config.json).
    "STATION_NAME",
    "STATION_CAPACITY",
    "AGG_WINDOW_SECONDS",
    "PCT_LOW",
    "PCT_HIGH",
    "PCT_CRITICAL",
}


def _env_keys(path: pathlib.Path) -> set[str]:
    keys = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        keys.add(key)
    return keys


def test_env_example_covers_every_spec_key():
    keys = _env_keys(REPO_ROOT / ".env_example")
    missing = SPEC_ENV_KEYS - keys
    assert not missing, f".env_example missing required keys: {sorted(missing)}"


# --- domain constants now live in .env (no separate demo.config.json) ------

DOMAIN_ENV_KEYS = {
    "STATION_NAME", "STATION_CAPACITY", "AGG_WINDOW_SECONDS",
    "PCT_LOW", "PCT_HIGH", "PCT_CRITICAL",
}


def test_env_example_has_domain_constants():
    keys = _env_keys(REPO_ROOT / ".env_example")
    missing = DOMAIN_ENV_KEYS - keys
    assert not missing, f".env_example missing domain constants: {sorted(missing)}"


def test_reference_exposes_domain_constants():
    assert isinstance(ref.STATION_NAME, str) and ref.STATION_NAME
    assert isinstance(ref.STATION_CAPACITY, int) and ref.STATION_CAPACITY > 0
    assert isinstance(ref.AGG_WINDOW_SECONDS, int) and ref.AGG_WINDOW_SECONDS > 0
    assert 0 < ref.PCT_LOW < ref.PCT_HIGH < ref.PCT_CRITICAL <= 1.0

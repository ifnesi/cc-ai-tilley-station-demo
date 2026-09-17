"""Load the Avro schemas that live in ``emulator/avro/*.avsc``.

Shared by the emulator (producers), ``tools/mockfeed.py`` (synthetic Avro),
and the Phase-0 round-trip test. The backend's live consumers fetch schemas
from Schema Registry, so they do not need this loader.
"""

from __future__ import annotations

import json
import pathlib

AVRO_DIR = pathlib.Path(__file__).resolve().parent / "avro"

# Raw topics (emulator -> Kafka) and derived topics (Flink -> Kafka),
RAW_TOPICS: tuple[str, ...] = (
    "train_in_transit",
    "train_in_station",
    "passengers_flow",
    "station_occupancy",
    "demo_control",
    "operator_questions",
)
DERIVED_TOPICS: tuple[str, ...] = (
    "station_metrics",
    "station_anomalies",
    "signal_state",
    "gateline_state",
    "station_ai_suggestions",
    "operator_answers",
)
ALL_TOPICS: tuple[str, ...] = RAW_TOPICS + DERIVED_TOPICS


def schema_path(topic: str) -> pathlib.Path:
    return AVRO_DIR / f"{topic}.avsc"


def load_schema_str(topic: str) -> str:
    """Raw .avsc text — what confluent-kafka's AvroSerializer expects."""
    return schema_path(topic).read_text(encoding="utf-8")


def load_schema(topic: str) -> dict:
    """Parsed schema dict."""
    return json.loads(load_schema_str(topic))


def all_schemas() -> dict[str, dict]:
    return {topic: load_schema(topic) for topic in ALL_TOPICS}

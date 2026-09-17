"""Event producer abstraction.

The emulator emits through an ``EventProducer`` so it can run against Confluent
Cloud (``KafkaEventProducer``) or entirely in memory for the Phase-1 stop gate
(``InMemoryEventProducer``) — the "fake/in-memory producer, no cloud needed".
"""

from __future__ import annotations

import datetime as dt
import threading
from typing import Optional, Protocol

from . import reference as ref
from .schemas import load_schema, load_schema_str

# The emulator produces the four raw display topics AND the two control-state
# topics (signals + gateline are now deterministic, emulator-controlled). Flink
# owns the derived analytics topics (metrics, anomalies, AI suggestions);
# demo_control is consumed.
EMULATOR_PRODUCED_TOPICS = (
    "train_in_transit",
    "train_in_station",
    "passengers_flow",
    "station_occupancy",
    "signal_state",
    "gateline_state",
)


def now() -> dt.datetime:
    """Timezone-aware UTC now (Avro timestamp-millis encodes this cleanly)."""
    return dt.datetime.now(dt.timezone.utc)


class EventProducer(Protocol):
    def send(self, topic: str, record: dict) -> None: ...
    def flush(self, timeout: float = 5.0) -> None: ...


class InMemoryEventProducer:
    """Collects emitted records; optionally validates each against its schema."""

    def __init__(self, validate: bool = True):
        self._lock = threading.Lock()
        self.records: list[tuple[str, dict]] = []
        self._validate = validate
        self._schemas = {}
        if validate:
            import fastavro

            self._fastavro = fastavro
            for topic in EMULATOR_PRODUCED_TOPICS:
                self._schemas[topic] = fastavro.parse_schema(load_schema(topic))

    def send(self, topic: str, record: dict) -> None:
        if self._validate:
            self._fastavro.validation.validate(
                record, self._schemas[topic], raise_errors=True
            )
        with self._lock:
            self.records.append((topic, record))

    def flush(self, timeout: float = 5.0) -> None:
        pass

    # test helpers
    def by_topic(self, topic: str) -> list[dict]:
        with self._lock:
            return [r for t, r in self.records if t == topic]

    def count(self, topic: str) -> int:
        return len(self.by_topic(topic))

    def clear(self) -> None:
        with self._lock:
            self.records.clear()


class KafkaEventProducer:
    """Real Confluent Cloud producer with an AvroSerializer per topic."""

    def __init__(self, config):
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        sr = SchemaRegistryClient(config.schema_registry_config())
        self._serializers = {
            topic: AvroSerializer(sr, load_schema_str(topic))
            for topic in EMULATOR_PRODUCED_TOPICS
        }
        self._producer = Producer(config.producer_config())

    def send(self, topic: str, record: dict) -> None:
        from confluent_kafka.serialization import MessageField, SerializationContext

        ctx = SerializationContext(topic, MessageField.VALUE)
        value = self._serializers[topic](record, ctx)
        self._producer.produce(topic, value=value)
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)


# --- record builders (match the Phase-0 schemas) --------------------------


def foot_record(station_name: str, passengers: int) -> dict:
    return {"station_name": station_name, "passengers": passengers, "event_time": now()}


def occupancy_record(station_name: str, occupancy: int, capacity: int) -> dict:
    return {
        "station_name": station_name,
        "occupancy": max(0, occupancy),
        "capacity": capacity,
        "event_time": now(),
    }


def approach_record(
    service_id: str,
    direction: str,
    passengers: int,
    train_type: str,
    train_capacity: int,
    eta: int,
) -> dict:
    return {
        "service_id": service_id,
        "direction_of_travel": direction,
        "next_stop": ref.STATION_NAME,          # arriving
        "passengers": passengers,
        "passengers_boarding": None,            # approach
        "train_type": train_type,
        "train_capacity": train_capacity,
        "eta": eta,
        "event_time": now(),
    }


def in_station_record(
    service_id: str,
    direction: str,
    staying: int,
    out: int,
    station: int,
    train_type: str,
) -> dict:
    return {
        "service_id": service_id,
        "direction_of_travel": direction,
        "passengers_staying": staying,
        "passengers_out": out,
        "passengers_station": station,
        "train_type": train_type,
        "event_time": now(),
    }


def depart_record(
    service_id: str,
    direction: str,
    staying: int,
    boarding: int,
    train_type: str,
    train_capacity: int,
) -> dict:
    return {
        "service_id": service_id,
        "direction_of_travel": direction,
        "next_stop": ref.departs_to(direction),  # far neighbour
        "passengers": staying + boarding,
        "passengers_boarding": boarding,
        "train_type": train_type,
        "train_capacity": train_capacity,
        "eta": 0,
        "event_time": now(),
    }


def signal_record(station_name: str, side: str, state: str, reason: str, occupancy: int) -> dict:
    return {
        "station_name": station_name,
        "side": side,               # LEFT gates east_bound, RIGHT gates west_bound
        "state": state,             # RED | GREEN
        "reason": reason,
        "occupancy_at_decision": max(0, occupancy),
        "event_time": now(),
    }


def gateline_record(station_name: str, state: str, throttle_factor: float, reason: str) -> dict:
    return {
        "station_name": station_name,
        "state": state,                     # OPEN | RESTRICTED | CLOSED
        "throttle_factor": float(throttle_factor),
        "reason": reason,
        "event_time": now(),
    }

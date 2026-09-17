"""demo_control producer.

The presenter's "Trigger rush hour" / "Reset" buttons POST to the backend,
which publishes a `demo_control` record the emulator consumes. The record
builder is pure and validated; the Kafka wiring is injected so the unit stop
gate can assert the produced record without a broker.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Optional

log = logging.getLogger("tilley.producer")

VALID_COMMANDS = ("SURGE", "RESET")


class DemoControlError(ValueError):
    """Raised on invalid demo-control input (maps to HTTP 4xx)."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def build_demo_control(
    command: str,
    factor: Optional[float] = None,
    duration_seconds: Optional[int] = None,
) -> dict:
    """Build and validate a `demo_control` record (matches demo_control.avsc).

    Raises `DemoControlError` on bad input.
    """
    if command not in VALID_COMMANDS:
        raise DemoControlError(f"command must be one of {VALID_COMMANDS}, got {command!r}")

    if factor is not None:
        try:
            factor = float(factor)
        except (TypeError, ValueError):
            raise DemoControlError("factor must be a number") from None
        if factor <= 0:
            raise DemoControlError("factor must be > 0")

    if duration_seconds is not None:
        if isinstance(duration_seconds, bool) or not isinstance(duration_seconds, int):
            # accept numeric strings / floats that are whole numbers
            try:
                as_float = float(duration_seconds)
            except (TypeError, ValueError):
                raise DemoControlError("duration_seconds must be an integer") from None
            if not as_float.is_integer():
                raise DemoControlError("duration_seconds must be an integer")
            duration_seconds = int(as_float)
        if duration_seconds <= 0:
            raise DemoControlError("duration_seconds must be > 0")

    return {
        "command": command,
        "factor": factor,
        "duration_seconds": duration_seconds,
        "event_time": _now(),
    }


class DemoControlProducer:
    """Serialises a `demo_control` record and produces it to Kafka."""

    def __init__(self, producer: Any, serializer: Any, topic: str = "demo_control"):
        self._producer = producer
        self._serializer = serializer
        self._topic = topic

    @classmethod
    def from_config(cls, config) -> "DemoControlProducer":
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer

        from emulator.schemas import load_schema_str

        sr = SchemaRegistryClient(config.schema_registry_config())
        serializer = AvroSerializer(sr, load_schema_str(config.demo_control_topic))
        producer = Producer(config.producer_config())
        return cls(producer, serializer, topic=config.demo_control_topic)

    def send(
        self,
        command: str,
        factor: Optional[float] = None,
        duration_seconds: Optional[int] = None,
    ) -> dict:
        """Build, serialise and produce a demo_control record. Returns the record."""
        from confluent_kafka.serialization import MessageField, SerializationContext

        record = build_demo_control(command, factor, duration_seconds)
        ctx = SerializationContext(self._topic, MessageField.VALUE)
        value = self._serializer(record, ctx)
        self._producer.produce(self._topic, value=value)
        self._producer.flush(5)
        log.info("Produced demo_control %s (factor=%s, duration=%s)", command, factor, duration_seconds)
        return record

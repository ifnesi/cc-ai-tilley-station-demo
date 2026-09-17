"""Kafka -> Socket.IO fan-out.

Two layers:

* Pure functions (`build_message`, `emit_record`) map a deserialised record to
  a Socket.IO channel + JSON-safe payload. These have no Kafka dependency and
  are what the Phase-3 unit stop gate exercises.
* `ConsumerManager` runs one `confluent-kafka` consumer thread per topic, each
  deserialising Avro (schema fetched from Schema Registry) and calling
  `emit_record`. State is bounded — no buffering, each message is emitted and
  dropped.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
from typing import Any, Callable, Iterable

log = logging.getLogger("tilley.consumers")

# topic -> Socket.IO channel. The three raw event topics multiplex onto
# one 'raw_event' channel; the frontend distinguishes them by 'event_type'.
TOPIC_CHANNELS: dict[str, str] = {
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

RAW_EVENT_TOPICS: frozenset[str] = frozenset(
    t for t, ch in TOPIC_CHANNELS.items() if ch == "raw_event"
)
CONSUMED_TOPICS: tuple[str, ...] = tuple(TOPIC_CHANNELS)


def to_wire(value: Any) -> Any:
    """Make an Avro-deserialised value JSON-serialisable for Socket.IO.

    Avro's ``timestamp-millis`` logical type deserialises to a ``datetime``;
    convert those to epoch milliseconds (ints) so the browser gets numbers.
    Enums already come back as strings.
    """
    if isinstance(value, dt.datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, dict):
        return {k: to_wire(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_wire(v) for v in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def build_message(topic: str, record: dict) -> tuple[str, dict]:
    """Return ``(channel, payload)`` for a deserialised record from ``topic``."""
    channel = TOPIC_CHANNELS[topic]
    data = to_wire(record)
    if channel == "raw_event":
        return channel, {"event_type": topic, "data": data}
    return channel, data


def emit_record(socketio, topic: str, record: dict) -> tuple[str, dict]:
    """Build and emit a record on its Socket.IO channel."""
    channel, payload = build_message(topic, record)
    socketio.emit(channel, payload)
    return channel, payload


# --- Kafka consumer threads ------------------------------------------------


class ConsumerManager:
    """Runs one consumer thread per topic, emitting to Socket.IO.

    Construction is side-effect free; ``start()`` opens the Kafka clients, so
    tests can import and construct this without a broker.
    """

    def __init__(
        self,
        socketio,
        config,
        topics: Iterable[str] = CONSUMED_TOPICS,
        consumer_factory: Callable[[str], Any] | None = None,
        deserializer_factory: Callable[[], Any] | None = None,
        on_record: Callable[[str, dict], None] | None = None,
    ):
        self._socketio = socketio
        self._config = config
        self._topics = tuple(topics)
        self._consumer_factory = consumer_factory
        self._deserializer_factory = deserializer_factory
        # Optional side-channel called with every (topic, record) — used to keep
        # the latest metrics/anomaly for the Ask-AI context snapshot.
        self._on_record = on_record
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()

    # Factories are injectable so tests can supply fakes; the defaults build
    # real Confluent Cloud clients and are only touched inside start().
    def _default_deserializer(self):
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        sr = SchemaRegistryClient(self._config.schema_registry_config())
        # No schema arg: the reader schema is resolved from the message's
        # embedded schema id, so one deserialiser handles every topic.
        return AvroDeserializer(sr)

    def _default_consumer(self, topic: str):
        from confluent_kafka import Consumer

        consumer = Consumer(self._config.consumer_config(group_suffix=topic))
        consumer.subscribe([topic])
        return consumer

    def start(self) -> None:
        if self._threads:
            raise RuntimeError("ConsumerManager already started")
        deser_factory = self._deserializer_factory or self._default_deserializer
        cons_factory = self._consumer_factory or self._default_consumer
        deserializer = deser_factory()
        for topic in self._topics:
            t = threading.Thread(
                target=self._run_topic,
                args=(topic, cons_factory, deserializer),
                name=f"consumer-{topic}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        log.info("Started %d consumer threads: %s", len(self._threads), ", ".join(self._topics))

    def _run_topic(self, topic: str, consumer_factory, deserializer) -> None:
        from confluent_kafka.serialization import MessageField, SerializationContext

        try:
            consumer = consumer_factory(topic)
        except Exception:  # pragma: no cover - connection failures at runtime
            log.exception("Failed to create consumer for %s", topic)
            return

        ctx = SerializationContext(topic, MessageField.VALUE)
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    log.warning("[%s] consumer error: %s", topic, msg.error())
                    continue
                try:
                    record = deserializer(msg.value(), ctx)
                    if record is None:
                        continue
                    if self._on_record is not None:
                        try:
                            self._on_record(topic, record)
                        except Exception:
                            log.exception("[%s] on_record hook failed", topic)
                    emit_record(self._socketio, topic, record)
                except Exception:
                    log.exception("[%s] failed to handle message", topic)
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover
                pass
            log.info("[%s] consumer stopped", topic)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=3)
        self._threads.clear()

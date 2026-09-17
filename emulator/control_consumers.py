"""Presenter control consumer.

The emulator now owns the deterministic controls (signals + gateline) and
publishes them itself, so the only control it consumes is the presenter's
surge/reset from the UI:

* demo_control -> SURGE / RESET

Only started when Kafka is configured; the Phase-1 stop gate drives StationState
directly instead of going through a broker.
"""

from __future__ import annotations

import logging
import threading

log = logging.getLogger("tilley.control")


class ControlConsumers:
    def __init__(self, config, state):
        self.config = config
        self.state = state
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        sr = SchemaRegistryClient(self.config.schema_registry_config())
        deserializer = AvroDeserializer(sr)
        for topic, handler in (("demo_control", self._on_demo_control),):
            t = threading.Thread(
                target=self._run, args=(topic, deserializer, handler),
                name=f"control-{topic}", daemon=True,
            )
            t.start()
            self._threads.append(t)
        log.info("Control consumer started (demo_control)")

    def _run(self, topic, deserializer, handler) -> None:
        from confluent_kafka import Consumer
        from confluent_kafka.serialization import MessageField, SerializationContext

        consumer = Consumer(self.config.consumer_config(group_suffix=topic))
        consumer.subscribe([topic])
        ctx = SerializationContext(topic, MessageField.VALUE)
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                try:
                    record = deserializer(msg.value(), ctx)
                    if record is not None:
                        handler(record)
                except Exception:
                    log.exception("[%s] failed to handle message", topic)
        finally:
            consumer.close()

    # --- handlers (also unit-testable directly) ---------------------------

    def _on_demo_control(self, rec: dict) -> None:
        if rec["command"] == "SURGE":
            factor = rec.get("factor") or self.config.surge_factor
            duration = rec.get("duration_seconds") or self.config.surge_duration
            self.state.start_surge(factor, duration)
            log.info("demo_control SURGE x%.1f for %ss", factor, duration)
        elif rec["command"] == "RESET":
            self.state.reset()
            log.info("demo_control RESET")

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=3)

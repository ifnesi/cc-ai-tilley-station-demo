"""The control consumer threads.

Subscribe to signal_state, gateline_state, demo_control and station_ai_suggestions
and update the shared StationState (or act) so the emulator obeys Flink's control
loops, the AI advisor, and the presenter:

* signal_state          -> state.set_signal(side, RED|GREEN)   (trains hold at RED)
* gateline_state        -> state.set_throttle(throttle_factor) (caps street inflow)
* demo_control          -> SURGE / RESET
* station_ai_suggestions -> on the DISPATCH_RELIEF_TRAIN directive, dispatch the
                            standby relief train (AI as actuator)

Only started when Kafka is configured; the Phase-1 stop gate drives StationState
directly instead of going through a broker.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from . import reference as ref

log = logging.getLogger("tilley.control")


class ControlConsumers:
    def __init__(self, config, state, ai_directives: dict[str, Callable[[], object]] | None = None):
        self.config = config
        self.state = state
        # Maps an AI directive token (e.g. DISPATCH_RELIEF_TRAIN, DIVERT_TO_STREET)
        # to the emulator action it triggers. The emulator supplies this; each
        # directive present in a suggestion fires its action once.
        self._ai_directives = ai_directives or {}
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        sr = SchemaRegistryClient(self.config.schema_registry_config())
        deserializer = AvroDeserializer(sr)
        for topic, handler in (
            ("signal_state", self._on_signal),
            ("gateline_state", self._on_gateline),
            ("demo_control", self._on_demo_control),
            ("station_ai_suggestions", self._on_ai_suggestion),
        ):
            t = threading.Thread(
                target=self._run, args=(topic, deserializer, handler),
                name=f"control-{topic}", daemon=True,
            )
            t.start()
            self._threads.append(t)
        log.info(
            "Control consumers started (signal_state, gateline_state, demo_control, "
            "station_ai_suggestions)"
        )

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

    def _on_signal(self, rec: dict) -> None:
        self.state.set_signal(rec["side"], rec["state"])

    def _on_gateline(self, rec: dict) -> None:
        self.state.set_throttle(rec["throttle_factor"])

    def _on_demo_control(self, rec: dict) -> None:
        if rec["command"] == "SURGE":
            factor = rec.get("factor") or self.config.surge_factor
            duration = rec.get("duration_seconds") or self.config.surge_duration
            self.state.start_surge(factor, duration)
            log.info("demo_control SURGE x%.1f for %ss", factor, duration)
        elif rec["command"] == "RESET":
            self.state.reset()
            log.info("demo_control RESET")

    def _on_ai_suggestion(self, rec: dict) -> None:
        # The advisor appends machine directive line(s); actuate each one present.
        # Match is substring-based so it is robust to surrounding punctuation /
        # whitespace from the model.
        suggestion = rec.get("suggestion") or ""
        for token, action in self._ai_directives.items():
            if token in suggestion:
                try:
                    action()
                except Exception:
                    log.exception("AI directive %s failed", token)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=3)

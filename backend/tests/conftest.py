"""Shared fixtures and broker-free fakes for the Phase-3 backend stop gate."""

from __future__ import annotations

import itertools
from typing import Optional

import pytest

from backend.app import create_app
from backend.producer import build_demo_control


class RecordingSocketIO:
    """Stand-in for a SocketIO instance that records emits (no server)."""

    def __init__(self):
        self.emits: list[tuple[str, object]] = []

    def emit(self, channel, payload=None, **kwargs):
        self.emits.append((channel, payload))


class FakeDemoProducer:
    """Validates like the real producer (build_demo_control) but skips Kafka.

    Captures the well-formed record so tests can assert on it; still raises
    DemoControlError on bad input, exercising the endpoint's 4xx path.
    """

    def __init__(self):
        self.sent: list[dict] = []

    def send(self, command, factor=None, duration_seconds=None):
        record = build_demo_control(command, factor=factor, duration_seconds=duration_seconds)
        self.sent.append(record)
        return record


@pytest.fixture
def fake_producer():
    return FakeDemoProducer()


@pytest.fixture
def app_and_socketio(fake_producer, tmp_path_factory):
    """create_app wired with the fake producer and the real frontend dir."""
    from backend.config import FRONTEND_DIR

    app, socketio = create_app(
        demo_producer=fake_producer,
        async_mode="threading",
        frontend_dir=FRONTEND_DIR,
    )
    return app, socketio


@pytest.fixture
def client(app_and_socketio):
    app, _ = app_and_socketio
    return app.test_client()


# --- fakes for the broker-free ConsumerManager integration test ------------


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value

    def error(self):
        return None


class FakeConsumer:
    """Returns one message, then None forever (idle poll)."""

    def __init__(self, topic: str):
        self.topic = topic
        self._messages = itertools.chain([FakeMessage(topic.encode())], itertools.repeat(None))
        self.closed = False

    def poll(self, timeout: float = 0.0) -> Optional[FakeMessage]:
        return next(self._messages)

    def close(self):
        self.closed = True


def fake_consumer_factory(topic: str) -> FakeConsumer:
    return FakeConsumer(topic)


def fake_deserializer_factory():
    """Deserialiser that returns a sample record for the message's topic."""
    from emulator.samples import sample_record

    def _deserialize(value, ctx):
        return sample_record(ctx.topic)

    return _deserialize

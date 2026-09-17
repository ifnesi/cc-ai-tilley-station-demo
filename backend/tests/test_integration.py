"""Phase-3 integration stop gate (broker-free).

Drives the real ConsumerManager threads with injected fake consumers that feed
one sample record per topic, and asserts:

* the consumer layer emits on every channel (deterministic, RecordingSocketIO);
* a real Socket.IO test client connects and receives a message on every
  channel within a few seconds.

The full against-the-cloud version (mockfeed.py -> Confluent -> backend) is run
manually per the README; this exercises the same wiring without a broker.
"""

from __future__ import annotations

import time

from backend.app import create_app
from backend.config import FRONTEND_DIR
from backend.consumers import CONSUMED_TOPICS, ConsumerManager
from backend.tests.conftest import (
    RecordingSocketIO,
    fake_consumer_factory,
    fake_deserializer_factory,
)

EXPECTED_CHANNELS = {"raw_event", "occupancy", "metrics", "anomaly", "signal", "gateline", "ai_suggestion"}


def _make_manager(socketio):
    return ConsumerManager(
        socketio,
        config=None,
        topics=CONSUMED_TOPICS,
        consumer_factory=fake_consumer_factory,
        deserializer_factory=fake_deserializer_factory,
    )


def test_consumer_layer_emits_every_channel():
    sio = RecordingSocketIO()
    manager = _make_manager(sio)
    manager.start()
    try:
        deadline = time.time() + 5
        while time.time() < deadline and len(sio.emits) < len(CONSUMED_TOPICS):
            time.sleep(0.05)
    finally:
        manager.stop()

    channels = {ch for ch, _ in sio.emits}
    assert EXPECTED_CHANNELS <= channels, f"missing channels: {EXPECTED_CHANNELS - channels}"


def test_socketio_client_receives_every_channel():
    app, socketio = create_app(demo_producer=None, async_mode="threading", frontend_dir=FRONTEND_DIR)
    client = socketio.test_client(app)
    assert client.is_connected()

    manager = _make_manager(socketio)
    manager.start()
    try:
        received_channels: set[str] = set()
        deadline = time.time() + 5
        while time.time() < deadline and not (EXPECTED_CHANNELS <= received_channels):
            for msg in client.get_received():
                received_channels.add(msg["name"])
            time.sleep(0.05)
    finally:
        manager.stop()
        client.disconnect()

    assert EXPECTED_CHANNELS <= received_channels, (
        f"client missing channels: {EXPECTED_CHANNELS - received_channels}"
    )

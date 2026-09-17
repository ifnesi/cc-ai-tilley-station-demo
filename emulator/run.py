"""Emulator entrypoint: wire real Confluent Cloud clients and run.

    python -m emulator.run

Reads the single root .env. Presenter surge is normally triggered via the
UI button -> backend -> demo_control -> the emulator's control consumer, but a
keyboard fallback is available in this terminal: press 's' + Enter to surge,
'r' + Enter to reset, 'q' + Enter to quit.
"""

from __future__ import annotations

import logging
import sys
import threading

from .config import EmulatorConfig
from .emulator import Emulator
from .producers import KafkaEventProducer
from .state import StationState

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("tilley.emulator.run")


def _keyboard_loop(emulator: Emulator, stop: threading.Event) -> None:
    if not sys.stdin or not sys.stdin.isatty():
        stop.wait()
        return
    log.info("Keyboard: 's'=surge, 'r'=reset, 'q'=quit")
    while not stop.is_set():
        try:
            line = sys.stdin.readline()
        except Exception:
            break
        if not line:
            break
        cmd = line.strip().lower()
        if cmd == "s":
            emulator.trigger_surge()
        elif cmd == "r":
            emulator.trigger_reset()
        elif cmd == "q":
            break
    stop.set()


def main() -> int:
    config = EmulatorConfig.from_env()
    if not config.kafka_configured:
        log.error(
            "Kafka/Schema Registry not configured. Set BOOTSTRAP_SERVERS and "
            "SCHEMA_REGISTRY_URL in the root .env (from `terraform output`)."
        )
        return 1

    state = StationState(
        capacity=config.capacity,
        evacuation_flow=config.evacuation_flow,
        initial_occupancy=config.initial_occupancy,
    )
    producer = KafkaEventProducer(config)
    emulator = Emulator(config, state, producer)

    stop = threading.Event()
    emulator.start()
    log.info(
        "Emulator running for %s (capacity %d, occupancy %d). Ctrl-C to stop.",
        config.station_name, config.capacity, state.occupancy,
    )
    try:
        _keyboard_loop(emulator, stop)
    except KeyboardInterrupt:
        pass
    finally:
        emulator.stop()
        log.info("Emulator stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())

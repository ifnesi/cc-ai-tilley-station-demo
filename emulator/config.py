"""Emulator configuration, loaded from the single root .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from . import reference as ref

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    load_dotenv(REPO_ROOT / ".env")


@dataclass
class EmulatorConfig:
    # Kafka + Schema Registry (from `terraform output`)
    bootstrap_servers: str = ""
    kafka_api_key: str = ""
    kafka_api_secret: str = ""
    schema_registry_url: str = ""
    schema_registry_api_key: str = ""
    schema_registry_api_secret: str = ""
    client_id: str = "tilley-emulator"

    # Station
    station_name: str = ref.STATION_NAME
    capacity: int = ref.STATION_CAPACITY

    # Generation rates
    foot_min: int = 1
    foot_max: int = 40
    train_min: int = 10
    train_max: int = 40
    dwell_min: int = 8
    dwell_max: int = 20
    board_fraction: float = 0.35

    # Timing knobs
    foot_interval: float = 5.0 # foot loop cadence (~5s,)
    signal_poll_interval: float = 1.0  # re-poll cadence while held at RED
    eta_min: int = 4
    eta_max: int = 12

    # Surge
    surge_factor: float = 5.0
    surge_duration: int = 45
    auto_surge_after: int = 0          # 0 = disabled

    # Occupancy the emulator starts at / returns to on RESET (mid-band).
    initial_occupancy_fraction: float = 0.45

    frontend_seed: int | None = None   # RNG seed (tests/repro); None = nondeterministic

    consumer_group: str = "tilley-emulator"

    @property
    def initial_occupancy(self) -> int:
        return int(self.capacity * self.initial_occupancy_fraction)

    @property
    def kafka_configured(self) -> bool:
        return bool(self.bootstrap_servers and self.schema_registry_url)

    @classmethod
    def from_env(cls) -> "EmulatorConfig":
        load_env()
        e = os.environ
        return cls(
            bootstrap_servers=e.get("BOOTSTRAP_SERVERS", ""),
            kafka_api_key=e.get("KAFKA_API_KEY", ""),
            kafka_api_secret=e.get("KAFKA_API_SECRET", ""),
            schema_registry_url=e.get("SCHEMA_REGISTRY_URL", ""),
            schema_registry_api_key=e.get("SCHEMA_REGISTRY_API_KEY", ""),
            schema_registry_api_secret=e.get("SCHEMA_REGISTRY_API_SECRET", ""),
            # station_name/capacity come from reference.py (which reads .env).
            foot_min=int(e.get("FOOT_MIN", "1")),
            foot_max=int(e.get("FOOT_MAX", "40")),
            train_min=int(e.get("TRAIN_MIN", "10")),
            train_max=int(e.get("TRAIN_MAX", "40")),
            dwell_min=int(e.get("DWELL_MIN", "8")),
            dwell_max=int(e.get("DWELL_MAX", "20")),
            board_fraction=float(e.get("BOARD_FRACTION", "0.35")),
            surge_factor=float(e.get("SURGE_FACTOR", "5")),
            surge_duration=int(e.get("SURGE_DURATION", "45")),
            auto_surge_after=int(e.get("AUTO_SURGE_AFTER", "0")),
            # Occupancy the station starts at / returns to on RESET, as a fraction
            # of capacity. Clamped to [0, 1]; default 0.45 (a healthy mid-band).
            initial_occupancy_fraction=min(
                1.0, max(0.0, float(e.get("INITIAL_OCCUPANCY_FRACTION", "0.45")))
            ),
            consumer_group=e.get("CONSUMER_GROUP", "tilley-emulator"),
        )

    # --- confluent-kafka client configs -----------------------------------

    def kafka_common(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_api_key,
            "sasl.password": self.kafka_api_secret,
        }

    def producer_config(self) -> dict:
        return {**self.kafka_common(), "client.id": self.client_id, "linger.ms": 5}

    def consumer_config(self, group_suffix: str = "") -> dict:
        group = self.consumer_group + (f"-{group_suffix}" if group_suffix else "")
        return {
            **self.kafka_common(),
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            # signal_state / gateline_state are written transactionally by Flink;
            # read_uncommitted lets the emulator obey control changes within a few
            # seconds instead of waiting for Flink's ~1-min checkpoint commit.
            "isolation.level": "read_uncommitted",
        }

    def schema_registry_config(self) -> dict:
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": (
                f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}"
            ),
        }

"""Backend configuration, loaded from the single root .env."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from emulator import reference as ref

REPO_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = REPO_ROOT / "frontend"


def load_env() -> None:
    """Load the repo-root .env if present (real secrets are git-ignored)."""
    load_dotenv(REPO_ROOT / ".env")


@dataclass
class Config:
    # Kafka + Schema Registry (from `terraform output`)
    bootstrap_servers: str = ""
    kafka_api_key: str = ""
    kafka_api_secret: str = ""
    schema_registry_url: str = ""
    schema_registry_api_key: str = ""
    schema_registry_api_secret: str = ""
    consumer_group: str = "tilley-demo-backend"

    # HTTP server
    host: str = "0.0.0.0"
    port: int = 8080

    # SocketIO async mode. Default 'threading' because the Kafka consumers make
    # blocking librdkafka poll() calls in OS threads; threading mode emits from
    # arbitrary threads cleanly. Set to 'eventlet' only if you know you need it.
    async_mode: str = "threading"

    demo_control_topic: str = "demo_control"
    agg_window_seconds: int = ref.AGG_WINDOW_SECONDS  # from .env via reference.py

    # Surge defaults (used when the presenter does not override,).
    surge_factor: float = 5.0
    surge_duration: int = 45

    frontend_dir: Path = field(default=FRONTEND_DIR)

    @property
    def kafka_configured(self) -> bool:
        return bool(self.bootstrap_servers and self.schema_registry_url)

    @classmethod
    def from_env(cls) -> "Config":
        load_env()
        e = os.environ
        return cls(
            bootstrap_servers=e.get("BOOTSTRAP_SERVERS", ""),
            kafka_api_key=e.get("KAFKA_API_KEY", ""),
            kafka_api_secret=e.get("KAFKA_API_SECRET", ""),
            schema_registry_url=e.get("SCHEMA_REGISTRY_URL", ""),
            schema_registry_api_key=e.get("SCHEMA_REGISTRY_API_KEY", ""),
            schema_registry_api_secret=e.get("SCHEMA_REGISTRY_API_SECRET", ""),
            consumer_group=e.get("CONSUMER_GROUP", "tilley-demo-backend"),
            # agg_window_seconds comes from reference.py (which reads it from .env)
            host=e.get("BACKEND_HOST", "0.0.0.0"),
            port=int(e.get("BACKEND_PORT", "8080")),
            async_mode=e.get("SOCKETIO_ASYNC_MODE", "threading"),
            surge_factor=float(e.get("SURGE_FACTOR", "5")),
            surge_duration=int(e.get("SURGE_DURATION", "45")),
        )

    # --- confluent-kafka client configs -----------------------------------

    def kafka_common(self) -> dict:
        """Base client config with SASL_SSL / PLAIN auth for Confluent Cloud."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_api_key,
            "sasl.password": self.kafka_api_secret,
        }

    def producer_config(self) -> dict:
        return {**self.kafka_common(), "linger.ms": 5}

    def consumer_config(self, group_suffix: str = "") -> dict:
        group = self.consumer_group + (f"-{group_suffix}" if group_suffix else "")
        return {
            **self.kafka_common(),
            "group.id": group,
            # Live dashboard: start from the newest events, do not replay history.
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            # Flink writes the derived topics transactionally (exactly-once), so a
            # default read_committed consumer only sees them when Flink commits its
            # checkpoint (~1 min). read_uncommitted surfaces them immediately, so
            # metrics / signals / gateline / anomalies / AI reach the UI in seconds.
            "isolation.level": "read_uncommitted",
        }

    def schema_registry_config(self) -> dict:
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": (
                f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}"
            ),
        }

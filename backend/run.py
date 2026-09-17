"""Production entrypoint: wire real Confluent Cloud clients and serve.

    python -m backend.run

Reads the single root .env. If ``SOCKETIO_ASYNC_MODE=eventlet`` the
eventlet monkey-patch must run before anything else is imported, so this file
does it first, guarded, before importing the app factory.
"""

from __future__ import annotations

import logging
import os
import sys

# Eventlet must monkey-patch the stdlib before other imports. Default mode is
# 'threading' (robust with blocking librdkafka poll), so this is opt-in.
if os.environ.get("SOCKETIO_ASYNC_MODE") == "eventlet":  # pragma: no cover
    import eventlet

    eventlet.monkey_patch()

from backend.app import create_app  # noqa: E402
from backend.config import Config  # noqa: E402
from backend.consumers import ConsumerManager  # noqa: E402
from backend.producer import DemoControlProducer, QuestionProducer  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("tilley.run")


def main() -> int:
    config = Config.from_env()
    if not config.kafka_configured:
        log.error(
            "Kafka/Schema Registry not configured. Set BOOTSTRAP_SERVERS and "
            "SCHEMA_REGISTRY_URL in the root .env (from `terraform output`)."
        )
        return 1

    demo_producer = DemoControlProducer.from_config(config)
    question_producer = QuestionProducer.from_config(config)
    app, socketio = create_app(
        demo_producer=demo_producer,
        question_producer=question_producer,
        config=config,
        async_mode=config.async_mode,
    )

    manager = ConsumerManager(socketio, config)
    manager.start()

    log.info(
        "Serving Tilley dashboard on http://%s:%d (async_mode=%s)",
        config.host,
        config.port,
        config.async_mode,
    )
    try:
        socketio.run(
            app,
            host=config.host,
            port=config.port,
            allow_unsafe_werkzeug=True,
        )
    finally:
        manager.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

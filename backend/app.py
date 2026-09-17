"""Flask-SocketIO backend.

* Serves the static React frontend at ``/`` (single origin, no build step).
* Exposes ``POST /demo/surge`` and ``POST /demo/reset`` which publish to the
  ``demo_control`` topic via an injected :class:`DemoControlProducer`.
* Fans out every Kafka topic to Socket.IO channels via :class:`ConsumerManager`.

``create_app`` is import-safe and needs no broker: the Kafka producer and
consumer manager are injected, so the unit stop gate constructs the app with
fakes. ``backend/run.py`` wires the real Confluent Cloud clients.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO

from emulator import reference as ref

from .config import FRONTEND_DIR, Config
from .producer import DemoControlError, QuestionError

log = logging.getLogger("tilley.app")


def create_app(
    *,
    demo_producer=None,
    question_producer=None,
    latest_state=None,
    config: Optional[Config] = None,
    async_mode: str = "threading",
    frontend_dir: Optional[Path] = None,
) -> tuple[Flask, SocketIO]:
    config = config or Config()
    frontend_dir = Path(frontend_dir or config.frontend_dir)

    app = Flask(__name__, static_folder=None)
    app.config["FRONTEND_DIR"] = str(frontend_dir)
    app.demo_producer = demo_producer  # type: ignore[attr-defined]
    app.question_producer = question_producer  # type: ignore[attr-defined]
    app.latest_state = latest_state  # type: ignore[attr-defined]

    socketio = SocketIO(app, cors_allowed_origins="*", async_mode=async_mode)

    # --- demo-control endpoints -------------------------------------------

    def _dispatch(command: str):
        producer = app.demo_producer  # type: ignore[attr-defined]
        if producer is None:
            return jsonify(error="demo control unavailable: no producer configured"), 503

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return jsonify(error="request body must be a JSON object"), 400

        factor = body.get("factor")
        duration = body.get("duration_seconds")
        try:
            record = producer.send(command, factor=factor, duration_seconds=duration)
        except DemoControlError as exc:
            return jsonify(error=str(exc)), 400

        return (
            jsonify(
                status="accepted",
                command=record["command"],
                factor=record["factor"],
                duration_seconds=record["duration_seconds"],
            ),
            202,
        )

    @app.post("/demo/surge")
    def demo_surge():
        return _dispatch("SURGE")

    @app.post("/demo/reset")
    def demo_reset():
        return _dispatch("RESET")

    @app.post("/ask")
    def ask():
        producer = app.question_producer  # type: ignore[attr-defined]
        if producer is None:
            return jsonify(error="ask unavailable: no producer configured"), 503
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return jsonify(error="request body must be a JSON object"), 400
        state = app.latest_state  # type: ignore[attr-defined]
        context = state.context_string(ref.STATION_CAPACITY) if state is not None else ""
        try:
            record = producer.send(
                question=body.get("question", ""), station_name=ref.STATION_NAME, context=context
            )
        except QuestionError as exc:
            return jsonify(error=str(exc)), 400
        # The answer arrives asynchronously on the 'operator_answer' Socket.IO
        # channel (Flink -> operator_answers -> backend), correlated by question_id.
        return jsonify(status="accepted", question_id=record["question_id"], question=record["question"]), 202

    @app.get("/health")
    def health():
        return jsonify(status="ok", producer=app.demo_producer is not None), 200

    @app.get("/config")
    def config_endpoint():
        # Domain constants for the dashboard (from env via reference.py).
        return jsonify(
            station_name=ref.STATION_NAME,
            capacity=ref.STATION_CAPACITY,
            agg_window_seconds=ref.AGG_WINDOW_SECONDS,
            pct_low=ref.PCT_LOW,
            pct_high=ref.PCT_HIGH,
            pct_critical=ref.PCT_CRITICAL,
            neighbour_west=ref.NEIGHBOUR_WEST,
            neighbour_east=ref.NEIGHBOUR_EAST,
            surge_duration=config.surge_duration,
            surge_factor=config.surge_factor,
        ), 200

    # --- static frontend (no build step) ----------------------------------

    @app.get("/")
    def index():
        return send_from_directory(app.config["FRONTEND_DIR"], "index.html")

    @app.get("/<path:filename>")
    def static_files(filename: str):
        return send_from_directory(app.config["FRONTEND_DIR"], filename)

    return app, socketio

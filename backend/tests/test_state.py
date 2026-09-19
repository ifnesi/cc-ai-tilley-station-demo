"""Freshness rules for the Ask-AI live context snapshot."""

from __future__ import annotations

import datetime as dt

from backend.state import LatestState


def _metrics(at: dt.datetime) -> dict:
    return {
        "window_start": at,
        "occupancy": 700,
        "occupancy_pct": 700 / 1200,
        "foot_in": 20,
        "alight_total": 30,
        "board_total": 25,
    }


def _anomaly(at: dt.datetime) -> dict:
    return {
        "window_start": at,
        "metric": "foot_in",
        "actual": 80.0,
        "forecast": 20.0,
        "is_anomaly": True,
    }


def test_recent_anomaly_is_included_in_context():
    state = LatestState()
    now = dt.datetime.now(dt.timezone.utc)
    state.update("station_metrics", _metrics(now))
    state.update("station_anomalies", _anomaly(now - dt.timedelta(seconds=10)))

    context = state.context_string(1200, max_anomaly_age_seconds=60)

    assert "latest anomaly on foot_in" in context


def test_stale_anomaly_is_omitted_from_context():
    state = LatestState()
    now = dt.datetime.now(dt.timezone.utc)
    state.update("station_metrics", _metrics(now))
    state.update("station_anomalies", _anomaly(now - dt.timedelta(minutes=5)))

    context = state.context_string(1200, max_anomaly_age_seconds=60)

    assert "anomaly" not in context

"""One valid sample record per topic.

Used by the Phase-0 Avro round-trip test and as a shape reference for
``tools/mockfeed.py``. Timestamp fields (``event_time``, ``window_start``,
``window_end``) are timezone-aware ``datetime`` objects — the form Avro's
``timestamp-millis`` logical type encodes cleanly across fastavro and
confluent-kafka's serializers.
"""

from __future__ import annotations

import datetime as _dt

from . import reference as ref


def now() -> _dt.datetime:
    """Timezone-aware UTC 'now', millisecond-truncated."""
    t = _dt.datetime.now(_dt.timezone.utc)
    return t.replace(microsecond=(t.microsecond // 1000) * 1000)


def sample_record(topic: str) -> dict:
    """A single schema-valid example record for ``topic``."""
    t = now()
    builders = {
        "train_in_transit": lambda: {
            "service_id": "a1b2c3d4",
            "direction_of_travel": ref.EAST_BOUND,
            "next_stop": ref.STATION_NAME,          # approach
            "passengers": 180,
            "passengers_boarding": None,            # set only on departure
            "train_type": "C98",
            "train_capacity": ref.train_capacity("C98"),
            "eta": 12,
            "event_time": t,
        },
        "train_in_station": lambda: {
            "service_id": "a1b2c3d4",
            "direction_of_travel": ref.EAST_BOUND,
            "passengers_staying": 90,
            "passengers_out": 60,
            "passengers_station": 30,
            "train_type": "C98",
            "event_time": t,
        },
        "passengers_flow": lambda: {
            "station_name": ref.STATION_NAME,
            "passengers": 18,
            "event_time": t,
        },
        "station_occupancy": lambda: {
            "station_name": ref.STATION_NAME,
            "occupancy": 540,
            "capacity": ref.STATION_CAPACITY,
            "event_time": t,
        },
        "demo_control": lambda: {
            "command": "SURGE",
            "factor": 5.0,
            "duration_seconds": 45,
            "event_time": t,
        },
        "station_metrics": lambda: {
            "station_name": ref.STATION_NAME,
            "window_start": t,
            "window_end": t + _dt.timedelta(seconds=15),
            "foot_in": 120,
            "alight_wait": 40,
            "alight_exit": 80,
            "alight_total": 120,
            "board_total": 95,
            "alight_east": 70,
            "alight_west": 50,
            "board_east": 55,
            "board_west": 40,
            "net_change": 65,
            "occupancy": 540,
            "occupancy_pct": 540 / ref.STATION_CAPACITY,
        },
        "station_anomalies": lambda: {
            "window_start": t,
            "metric": "foot_in",
            "actual": 320.0,
            "forecast": 110.0,
            "lower_bound": 40.0,
            "upper_bound": 190.0,
            "is_anomaly": True,
            "occupancy": 1020,
            "occupancy_pct": 1020 / ref.STATION_CAPACITY,
        },
        "signal_state": lambda: {
            "station_name": ref.STATION_NAME,
            "side": ref.SIDE_LEFT,
            "state": "RED",
            "reason": "Holding arrivals: occupancy 88%, concourse control",
            "occupancy_at_decision": 1056,
            "event_time": t,
        },
        "gateline_state": lambda: {
            "station_name": ref.STATION_NAME,
            "state": "RESTRICTED",
            "throttle_factor": 0.4,
            "reason": "Occupancy 88% over HIGH threshold; restricting street inflow",
            "event_time": t,
        },
        "station_ai_suggestions": lambda: {
            "trigger": "threshold",
            "metric": "occupancy_pct",
            "severity": "HIGH",
            "suggestion": (
                "SEVERITY: HIGH\n"
                "ASSESSMENT: Occupancy at 96% driven by a foot-entry surge.\n"
                "ACTIONS: 1) Close the street gateline now. 2) Keep both signals "
                "green so waiting passengers can board out. 3) Station control and "
                "PA advising Overton and Jones as alternatives.\n"
                "WATCH: Occupancy trend and westbound platform loading."
            ),
            "event_time": t,
        },
        "operator_questions": lambda: {
            "question_id": "q-abc123",
            "question": "Should I open the second exit for the westbound crowd?",
            "station_name": ref.STATION_NAME,
            "context": "occupancy 1056 of 1200 (88%); foot entries last window 240; alighting 90; boarding 70; latest anomaly on foot_in: 240 vs expected 90",
            "event_time": t,
        },
        "operator_answers": lambda: {
            "question_id": "q-abc123",
            "question": "Should I open the second exit for the westbound crowd?",
            "answer": "Yes — with occupancy at 88% and a foot-entry spike, open the second exit and route westbound arrivals to it; keep the gateline restricted and watch the westbound platform.",
            "event_time": t,
        },
    }
    return builders[topic]()


def all_samples() -> dict[str, dict]:
    from .schemas import ALL_TOPICS

    return {topic: sample_record(topic) for topic in ALL_TOPICS}

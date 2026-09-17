"""Latest live crowd state, tracked from the Kafka consumers.

The backend already consumes station_metrics and station_anomalies for the
dashboard; it keeps the most recent of each here so the Ask-AI endpoint can
snapshot a compact context string into the operator question. Flink then prompts
Bedrock with that context (no stream join needed).
"""

from __future__ import annotations

import threading


class LatestState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._metrics: dict | None = None
        self._anomaly: dict | None = None

    def update(self, topic: str, record: dict) -> None:
        """Called by the consumer for every record; keeps the latest of each."""
        with self._lock:
            if topic == "station_metrics":
                self._metrics = record
            elif topic == "station_anomalies":
                self._anomaly = record

    def context_string(self, capacity: int) -> str:
        """A compact one-line snapshot of live crowd state for the AI prompt."""
        with self._lock:
            m, a = self._metrics, self._anomaly
        if not m:
            return "no live metrics yet"
        occ = int(m.get("occupancy") or 0)
        pct = float(m.get("occupancy_pct") or 0.0)
        parts = [
            f"occupancy {occ} of {capacity} ({round(pct * 100)}%)",
            f"foot entries last window {int(m.get('foot_in') or 0)}",
            f"alighting {int(m.get('alight_total') or 0)}",
            f"boarding {int(m.get('board_total') or 0)}",
        ]
        if a and a.get("is_anomaly"):
            parts.append(
                f"latest anomaly on {a.get('metric')}: "
                f"{round(float(a.get('actual') or 0))} vs expected {round(float(a.get('forecast') or 0))}"
            )
        return "; ".join(parts)

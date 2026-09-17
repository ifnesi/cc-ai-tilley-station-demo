"""Synthetic Avro feed for all topics.

Lets the backend and frontend be developed and tested without the real
emulator + Flink pipeline. It produces coherent, evolving traffic to the four
display raw topics and the five derived topics, runs *simplified* versions of
the control loops (emit-on-change) and ML anomaly flags so the dashboard
behaves realistically, and consumes ``demo_control`` so the presenter's
"Trigger rush hour" / "Reset" buttons drive a visible surge end to end.

Run (needs the root .env with Confluent Cloud creds and topics already created):

    python -m tools.mockfeed

This is a *mock*, not the source of truth: occupancy here is kept in a mid-band
by self-stabilising boarding, exactly like the real emulator, but the
numbers are illustrative.
"""

from __future__ import annotations

import argparse
import datetime as dt
import heapq
import itertools
import logging
import random
import threading
import time
from dataclasses import dataclass, field

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from backend.config import Config
from emulator import reference as ref
from emulator.schemas import load_schema_str

log = logging.getLogger("tilley.mockfeed")

# Topics the mock produces (everything except demo_control, which it consumes).
PRODUCED_TOPICS = (
    "train_in_transit",
    "train_in_station",
    "passengers_flow",
    "station_occupancy",
    "station_metrics",
    "station_anomalies",
    "signal_state",
    "gateline_state",
    "station_ai_suggestions",
)


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


@dataclass
class WindowAccum:
    """Per-metrics-window counters."""

    foot_in: int = 0
    alight_wait: int = 0
    alight_exit: int = 0
    board_total: int = 0
    alight_east: int = 0
    alight_west: int = 0
    board_east: int = 0
    board_west: int = 0
    start: dt.datetime = field(default_factory=_now)

    def reset(self, start: dt.datetime) -> None:
        self.__init__(start=start)  # type: ignore[misc]


class MockFeed:
    def __init__(self, config: Config, seed: int | None = None):
        self.cfg = config
        self.rng = random.Random(seed)
        self.capacity = ref.STATION_CAPACITY

        # thresholds
        self.pct_low = 0.70
        self.pct_high = 0.85
        self.pct_critical = 0.95

        # generation rates
        self.foot_min, self.foot_max = 1, 40
        self.train_min, self.train_max = 10, 40
        self.dwell_min, self.dwell_max = 8, 20
        self.board_fraction = 0.35
        self.surge_factor_default = config.surge_factor
        self.surge_duration_default = config.surge_duration

        # live state
        self.occupancy = int(self.capacity * 0.45)
        self.signal = {"LEFT": "GREEN", "RIGHT": "GREEN"}
        self.gateline = "OPEN"
        self.throttle = 1.0
        self.surge_until = 0.0
        self.surge_factor = 1.0
        self._ai_episode_active = False

        # anomaly detector: rolling stats on foot_in per window
        self._foot_hist: list[float] = []

        self.window = WindowAccum(start=_now())
        self._ids = (f"{i:08x}" for i in itertools.count(1))
        self._heap: list[tuple[float, int, callable]] = []
        self._seq = itertools.count()
        self._stop = threading.Event()

        self._serializers: dict[str, AvroSerializer] = {}
        self._producer: Producer | None = None

    # --- Kafka wiring ------------------------------------------------------

    def _connect(self) -> None:
        sr = SchemaRegistryClient(self.cfg.schema_registry_config())
        for topic in PRODUCED_TOPICS:
            self._serializers[topic] = AvroSerializer(sr, load_schema_str(topic))
        self._producer = Producer(self.cfg.producer_config())
        self._demo_deserializer = AvroDeserializer(sr)

    def _emit(self, topic: str, record: dict) -> None:
        assert self._producer is not None
        ctx = SerializationContext(topic, MessageField.VALUE)
        value = self._serializers[topic](record, ctx)
        self._producer.produce(topic, value=value)
        self._producer.poll(0)

    # --- scheduler ---------------------------------------------------------

    def _schedule(self, delay: float, fn) -> None:
        heapq.heappush(self._heap, (time.time() + delay, next(self._seq), fn))

    def _surge_active(self) -> bool:
        return time.time() < self.surge_until

    # --- producers ---------------------------------------------------------

    def _heartbeat(self) -> None:
        self._emit(
            "station_occupancy",
            {
                "station_name": ref.STATION_NAME,
                "occupancy": max(0, self.occupancy),
                "capacity": self.capacity,
                "event_time": _now(),
            },
        )

    def _foot_tick(self) -> None:
        factor = self.surge_factor if self._surge_active() else 1.0
        raw = round(self.rng.randint(self.foot_min, self.foot_max) * factor)
        entered = round(raw * self.throttle)
        if entered > 0:
            self.occupancy += entered
            self.window.foot_in += entered
            self._emit(
                "passengers_flow",
                {"station_name": ref.STATION_NAME, "passengers": entered, "event_time": _now()},
            )
        self._heartbeat()
        self._run_controls()
        self._schedule(self.rng.uniform(4, 6), self._foot_tick)

    def _spawn_train(self) -> None:
        direction = self.rng.choice(ref.DIRECTIONS)
        train_type = self.rng.choice(ref.TRAIN_TYPES)
        cap = ref.train_capacity(train_type)
        # surge biases toward fuller trains
        load_hi = cap if self._surge_active() else int(cap * 0.7)
        p = self.rng.randint(int(cap * 0.3), max(int(cap * 0.3) + 1, load_hi))
        p = min(p, cap)
        service_id = next(self._ids)
        side = ref.entry_side(direction)
        eta = self.rng.randint(4, 12)

        self._emit(
            "train_in_transit",
            {
                "service_id": service_id,
                "direction_of_travel": direction,
                "next_stop": ref.STATION_NAME,
                "passengers": p,
                "passengers_boarding": None,
                "train_type": train_type,
                "train_capacity": cap,
                "eta": eta,
                "event_time": _now(),
            },
        )
        self._heartbeat()
        self._schedule(eta, lambda: self._train_arrive(service_id, direction, train_type, cap, p, side))
        self._schedule(self.rng.randint(self.train_min, self.train_max), self._spawn_train)

    def _train_arrive(self, service_id, direction, train_type, cap, p, side) -> None:
        # hold at RED (re-check), released by CRITICAL->green or occupancy falling
        if self.signal[side] == "RED":
            self._schedule(1.0, lambda: self._train_arrive(service_id, direction, train_type, cap, p, side))
            return
        # split arriving load P = staying + out + station
        staying = int(p * self.rng.uniform(0.2, 0.4))
        out = int((p - staying) * self.rng.uniform(0.4, 0.6))
        station = p - staying - out
        self.occupancy += station
        self.window.alight_wait += station
        self.window.alight_exit += out
        if direction == ref.EAST_BOUND:
            self.window.alight_east += out + station
        else:
            self.window.alight_west += out + station
        self._emit(
            "train_in_station",
            {
                "service_id": service_id,
                "direction_of_travel": direction,
                "passengers_staying": staying,
                "passengers_out": out,
                "passengers_station": station,
                "train_type": train_type,
                "event_time": _now(),
            },
        )
        self._heartbeat()
        self._run_controls()
        dwell = self.rng.randint(self.dwell_min, self.dwell_max)
        self._schedule(dwell, lambda: self._train_depart(service_id, direction, train_type, cap, staying))

    def _train_depart(self, service_id, direction, train_type, cap, staying) -> None:
        desired = round(self.occupancy * self.board_fraction)
        boarding = max(0, min(desired, self.occupancy, cap - staying))
        self.occupancy -= boarding
        self.window.board_total += boarding
        if direction == ref.EAST_BOUND:
            self.window.board_east += boarding
        else:
            self.window.board_west += boarding
        self._emit(
            "train_in_transit",
            {
                "service_id": service_id,
                "direction_of_travel": direction,
                "next_stop": ref.departs_to(direction),
                "passengers": staying + boarding,
                "passengers_boarding": boarding,
                "train_type": train_type,
                "train_capacity": cap,
                "eta": 0,
                "event_time": _now(),
            },
        )
        self._heartbeat()
        self._run_controls()

    def _metrics_tick(self) -> None:
        w = self.window
        end = _now()
        alight_total = w.alight_wait + w.alight_exit
        occ = max(0, self.occupancy)
        occ_pct = occ / self.capacity
        self._emit(
            "station_metrics",
            {
                "station_name": self.station_name,
                "window_start": w.start,
                "window_end": end,
                "foot_in": w.foot_in,
                "alight_wait": w.alight_wait,
                "alight_exit": w.alight_exit,
                "alight_total": alight_total,
                "board_total": w.board_total,
                "alight_east": w.alight_east,
                "alight_west": w.alight_west,
                "board_east": w.board_east,
                "board_west": w.board_west,
                "net_change": w.foot_in + w.alight_wait - w.board_total,
                "occupancy": occ,
                "occupancy_pct": occ_pct,
            },
        )
        self._detect_anomaly("foot_in", w.foot_in, w.start)
        self.window.reset(end)
        self._schedule(self.cfg.agg_window_seconds, self._metrics_tick)

    # --- simplified ML + control loops ------------------------------------

    def _detect_anomaly(self, metric: str, actual: int, window_start: dt.datetime) -> None:
        self._foot_hist.append(float(actual))
        self._foot_hist = self._foot_hist[-40:] # bounded rolling window
        if len(self._foot_hist) < 8:
            return
        mean = sum(self._foot_hist) / len(self._foot_hist)
        var = sum((x - mean) ** 2 for x in self._foot_hist) / len(self._foot_hist)
        std = var ** 0.5
        lower, upper = mean - 3 * std, mean + 3 * std
        is_anom = actual > upper
        if is_anom:
            self._emit(
                "station_anomalies",
                {
                    "window_start": window_start,
                    "metric": metric,
                    "actual": float(actual),
                    "forecast": mean,
                    "lower_bound": lower,
                    "upper_bound": upper,
                    "is_anomaly": True,
                },
            )
            self._maybe_ai("anomaly", metric)

    def _run_controls(self) -> None:
        occ = max(0, self.occupancy)
        pct = occ / self.capacity

        # --- signal loop, both sides, emit on change ---------------
        for side in ("LEFT", "RIGHT"):
            desired = self.signal[side]
            if pct >= self.pct_critical:
                desired = "GREEN"  # deadlock-breaker: trains are the drain
                reason = f"CRITICAL {pct:.0%}: releasing trains to board people out"
            elif pct >= self.pct_high:
                desired = "RED"
                reason = f"Holding arrivals: occupancy {pct:.0%}, concourse control"
            elif pct < self.pct_low:
                desired = "GREEN"
                reason = f"Occupancy {pct:.0%} below threshold: normal running"
            else:
                reason = None
            if desired != self.signal[side] and reason is not None:
                self.signal[side] = desired
                self._emit(
                    "signal_state",
                    {
                        "station_name": ref.STATION_NAME,
                        "side": side,
                        "state": desired,
                        "reason": reason,
                        "occupancy_at_decision": occ,
                        "event_time": _now(),
                    },
                )
                if desired == "RED":
                    self._maybe_ai("threshold", "occupancy_pct")

        # --- gateline loop, emit on change -------------------------
        if pct >= self.pct_critical:
            g, thr, reason = "CLOSED", 0.0, f"CRITICAL {pct:.0%}: closing street gateline"
        elif pct >= self.pct_high:
            g, thr, reason = "RESTRICTED", 0.4, f"Occupancy {pct:.0%} over HIGH: restricting inflow"
        else:
            g, thr, reason = "OPEN", 1.0, f"Occupancy {pct:.0%} nominal: gateline open"
        if g != self.gateline:
            self.gateline = g
            self.throttle = thr
            self._emit(
                "gateline_state",
                {
                    "station_name": ref.STATION_NAME,
                    "state": g,
                    "throttle_factor": thr,
                    "reason": reason,
                    "event_time": _now(),
                },
            )
            if g != "OPEN":
                self._maybe_ai("threshold", "occupancy_pct")

        # AI episode resets when we return below LOW
        if pct < self.pct_low:
            self._ai_episode_active = False

    def _maybe_ai(self, trigger: str, metric: str) -> None:
        # Debounce: one Bedrock-style call per episode.
        if self._ai_episode_active:
            return
        self._ai_episode_active = True
        occ = max(0, self.occupancy)
        pct = occ / self.capacity
        severity = "HIGH" if pct >= self.pct_critical else ("MEDIUM" if pct >= self.pct_high else "LOW")
        suggestion = (
            f"SEVERITY: {severity}\n"
            f"ASSESSMENT: Occupancy at {pct:.0%} with a {trigger} on {metric}.\n"
            "ACTIONS: 1) Restrict or close the street gateline. 2) Keep signals green "
            "so waiting passengers board out. 3) Station control and PA advising Overton and Jones.\n"
            "WATCH: Occupancy trend and the busier platform loading."
        )
        self._emit(
            "station_ai_suggestions",
            {
                "trigger": trigger,
                "metric": metric,
                "severity": severity,
                "suggestion": suggestion,
                "event_time": _now(),
            },
        )

    # --- demo_control consumer --------------------------------------------

    def _consume_demo_control(self) -> None:
        consumer = Consumer(self.cfg.consumer_config(group_suffix="mockfeed-control"))
        consumer.subscribe(["demo_control"])
        ctx = SerializationContext("demo_control", MessageField.VALUE)
        try:
            while not self._stop.is_set():
                msg = consumer.poll(0.5)
                if msg is None or msg.error():
                    continue
                try:
                    rec = self._demo_deserializer(msg.value(), ctx)
                except Exception:
                    log.exception("failed to deserialize demo_control")
                    continue
                if rec is None:
                    continue
                self._apply_demo_control(rec)
        finally:
            consumer.close()

    def _apply_demo_control(self, rec: dict) -> None:
        if rec["command"] == "SURGE":
            factor = rec.get("factor") or self.surge_factor_default
            duration = rec.get("duration_seconds") or self.surge_duration_default
            self.surge_factor = float(factor)
            self.surge_until = time.time() + float(duration)
            log.info("SURGE x%.1f for %ss", self.surge_factor, duration)
        elif rec["command"] == "RESET":
            self.surge_until = 0.0
            self.surge_factor = 1.0
            self.occupancy = int(self.capacity * 0.45)
            log.info("RESET: surge cleared, occupancy returned to mid-band")

    # --- run loop ----------------------------------------------------------

    def run(self) -> None:
        self._connect()
        threading.Thread(target=self._consume_demo_control, name="demo-control", daemon=True).start()

        self._schedule(0, self._foot_tick)
        self._schedule(2, self._spawn_train)
        self._schedule(15, self._metrics_tick)
        self._heartbeat()
        log.info("mockfeed running — producing to %d topics; Ctrl-C to stop", len(PRODUCED_TOPICS))

        try:
            while not self._stop.is_set():
                if not self._heap:
                    time.sleep(0.05)
                    continue
                when, _, fn = self._heap[0]
                now = time.time()
                if when > now:
                    time.sleep(min(0.1, when - now))
                    continue
                heapq.heappop(self._heap)
                try:
                    fn()
                except Exception:
                    log.exception("scheduled task failed")
        except KeyboardInterrupt:
            pass
        finally:
            self._stop.set()
            if self._producer is not None:
                self._producer.flush(5)
            log.info("mockfeed stopped")


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic Avro feed for the Tilley demo")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible traffic")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = Config.from_env()
    if not config.kafka_configured:
        log.error("Kafka/Schema Registry not configured — set BOOTSTRAP_SERVERS and SCHEMA_REGISTRY_URL in .env")
        return 1
    MockFeed(config, seed=args.seed).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

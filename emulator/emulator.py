"""Threaded emulator orchestrator — the station's deterministic control system.

Owns the foot loop and the train spawn loop + per-train state machine, and it is
the authority for the two deterministic operational controls (no Flink/AI):

* Signals (block signalling): one train per platform per direction. While a train
  dwells, that side's signal is RED and following trains queue behind it in
  order; it goes GREEN when the platform clears and the next train advances.
* Gateline: throttles street inflow straight from occupancy bands
  (OPEN / RESTRICTED / CLOSED).

Both are published to Kafka (signal_state / gateline_state) so the dashboard and
Flink see them. The only control the emulator consumes is demo_control (the
presenter's surge/reset). Flink is purely analytics: windowed metrics, ML anomaly
detection, and an advisory GenAI message — it does not control the station.

The per-step methods (``foot_tick``, ``emit_approach``, ``arrive``, ``depart``)
are side-effect-scoped and reused by the fast simulator and the tests.
"""

from __future__ import annotations

import logging
import random
import threading
import uuid
from collections import deque

from . import model, producers, reference as ref
from .config import EmulatorConfig
from .producers import EventProducer
from .state import StationState

log = logging.getLogger("tilley.emulator")


class TrainSpec(dict):
    """Mutable per-train run state, keyed by a stable service_id."""


class Emulator:
    def __init__(
        self,
        config: EmulatorConfig,
        state: StationState,
        producer: EventProducer,
        rng: random.Random | None = None,
    ):
        self.config = config
        self.state = state
        self.producer = producer
        self.rng = rng or random.Random(config.frontend_seed)
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._trains: set[threading.Thread] = set()
        self._trains_lock = threading.Lock()

        # Block signalling: one train per platform per direction. A FIFO queue of
        # waiting service_ids per side + an "occupied" flag, coordinated by a
        # Condition, so trains enter the platform in arrival order and queue
        # behind the signal instead of overlapping.
        self._platform_cv = threading.Condition()
        self._platform_occupied = {ref.SIDE_LEFT: False, ref.SIDE_RIGHT: False}
        self._platform_queue = {ref.SIDE_LEFT: deque(), ref.SIDE_RIGHT: deque()}

        # Last published gateline state (for emit-on-change).
        self._gateline_state = "OPEN"

    # --- emit helpers ------------------------------------------------------

    def _emit_heartbeat(self) -> None:
        self.producer.send(
            "station_occupancy",
            producers.occupancy_record(
                self.config.station_name, self.state.occupancy, self.config.capacity
            ),
        )

    # --- foot loop ---------------------------------------------------------

    def foot_tick(self) -> int:
        """One foot-flow tick: set the gateline, admit street inflow, heartbeat."""
        # Deterministic gateline first, so the throttle it sets applies this tick.
        self._update_gateline()
        base = self.rng.randint(self.config.foot_min, self.config.foot_max)
        entered = self.state.admit_foot(base)  # admit_foot applies the throttle
        if entered > 0:
            self.producer.send(
                "passengers_flow",
                producers.foot_record(self.config.station_name, entered),
            )
        # Always heartbeat so occupancy is visible even when the gateline is
        # CLOSED (entered == 0), and so the analytics pipeline keeps flowing.
        self._emit_heartbeat()
        return entered

    # --- deterministic gateline control -----------------------------------

    def _update_gateline(self) -> None:
        """Set the street gateline straight from occupancy (deterministic rule).

        OPEN < HIGH <= RESTRICTED < CRITICAL <= CLOSED. Sets the throttle
        StationState applies to foot inflow and publishes gateline_state on
        change (emit-on-change) for the dashboard.
        """
        pct = self.state.occupancy_pct()
        if pct >= ref.PCT_CRITICAL:
            state, throttle = "CLOSED", ref.THROTTLE_CLOSED
        elif pct >= ref.PCT_HIGH:
            state, throttle = "RESTRICTED", ref.THROTTLE_RESTRICTED
        else:
            state, throttle = "OPEN", ref.THROTTLE_OPEN
        self.state.set_throttle(throttle)
        if state != self._gateline_state:
            self._gateline_state = state
            pctn = int(round(pct * 100))
            reason = {
                "CLOSED": f"CRITICAL {pctn}%: closing the street gateline",
                "RESTRICTED": f"Occupancy {pctn}% over HIGH: restricting street inflow",
                "OPEN": f"Occupancy {pctn}% nominal: gateline open",
            }[state]
            self.producer.send(
                "gateline_state",
                producers.gateline_record(self.config.station_name, state, throttle, reason),
            )

    def _foot_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.foot_tick()
            except Exception:
                log.exception("foot tick failed")
            if self._stop.wait(self.config.foot_interval):
                return

    # --- train lifecycle -------------------------------------------

    def new_train_spec(self) -> TrainSpec:
        direction = self.rng.choice(ref.DIRECTIONS)
        train_type, cap = model.pick_train(self.rng)
        p = model.arriving_load(self.rng, cap, self.state.surge_active())
        return TrainSpec(
            service_id=uuid.uuid4().hex[:8],
            direction=direction,
            train_type=train_type,
            capacity=cap,
            passengers=p,
            side=ref.entry_side(direction),
            eta=self.rng.randint(self.config.eta_min, self.config.eta_max),
            staying=0,
        )

    def emit_approach(self, spec: TrainSpec) -> None:
        self.producer.send(
            "train_in_transit",
            producers.approach_record(
                spec["service_id"], spec["direction"], spec["passengers"],
                spec["train_type"], spec["capacity"], spec["eta"],
            ),
        )
        self._emit_heartbeat()

    def arrive(self, spec: TrainSpec) -> None:
        split = model.split_arriving_load(spec["passengers"], self.rng)
        assert split.total == spec["passengers"], "arriving load not conserved"
        spec["staying"] = split.staying
        self.state.alight_to_wait(split.station)
        self.producer.send(
            "train_in_station",
            producers.in_station_record(
                spec["service_id"], spec["direction"],
                split.staying, split.out, split.station, spec["train_type"],
            ),
        )
        self._emit_heartbeat()

    def depart(self, spec: TrainSpec) -> int:
        spare = spec["capacity"] - spec["staying"]
        desired = model.desired_boarding(self.state.occupancy, self.config.board_fraction)
        boarding = self.state.board_out(desired, spare)
        self.producer.send(
            "train_in_transit",
            producers.depart_record(
                spec["service_id"], spec["direction"], spec["staying"],
                boarding, spec["train_type"], spec["capacity"],
            ),
        )
        self._emit_heartbeat()
        return boarding

    # --- deterministic block signalling -----------------------------------

    def _set_signal(self, side: str, state: str, reason: str) -> None:
        """Set a signal and publish signal_state on change (emit-on-change)."""
        if self.state.get_signal(side) == state:
            return
        self.state.set_signal(side, state)
        self.producer.send(
            "signal_state",
            producers.signal_record(
                self.config.station_name, side, state, reason, self.state.occupancy
            ),
        )

    def _acquire_platform(self, spec: TrainSpec) -> bool:
        """Join the side's FIFO queue and block until the platform is free and it
        is this train's turn. Sets the signal RED (platform now occupied). Returns
        False if the emulator stopped while waiting."""
        side = spec["side"]
        sid = spec["service_id"]
        with self._platform_cv:
            self._platform_queue[side].append(sid)
            while not self._stop.is_set() and not (
                not self._platform_occupied[side] and self._platform_queue[side][0] == sid
            ):
                self._platform_cv.wait(0.2)
            if self._stop.is_set():
                if sid in self._platform_queue[side]:
                    self._platform_queue[side].remove(sid)
                self._platform_cv.notify_all()
                return False
            self._platform_queue[side].popleft()
            self._platform_occupied[side] = True
        d = spec["direction"].replace("_", "-")
        self._set_signal(side, "RED", f"{d} train at the platform; holding arrivals")
        return True

    def _release_platform(self, spec: TrainSpec) -> None:
        """Free the platform, wake the next queued train, set the signal GREEN."""
        side = spec["side"]
        with self._platform_cv:
            self._platform_occupied[side] = False
            self._platform_cv.notify_all()
        self._set_signal(side, "GREEN", "Platform clear; arrivals released")

    def _train_task(self, spec: TrainSpec) -> None:
        acquired = False
        try:
            self.emit_approach(spec)
            if self._stop.wait(spec["eta"]):  # travel to the signal
                return
            # Block: wait for a free platform on this side, in arrival order.
            if not self._acquire_platform(spec):
                return
            acquired = True
            self.arrive(spec)
            dwell = self.rng.randint(self.config.dwell_min, self.config.dwell_max)
            if self._stop.wait(dwell):
                return
            self.depart(spec)
        except Exception:
            log.exception("train task failed")
        finally:
            if acquired:
                self._release_platform(spec)
            with self._trains_lock:
                self._trains.discard(threading.current_thread())

    def _spawn_train(self, spec: TrainSpec | None = None) -> None:
        spec = spec or self.new_train_spec()
        t = threading.Thread(target=self._train_task, args=(spec,), name=f"train-{spec['service_id']}", daemon=True)
        with self._trains_lock:
            self._trains.add(t)
        t.start()

    def _train_spawn_loop(self) -> None:
        while not self._stop.is_set():
            delay = self.rng.randint(self.config.train_min, self.config.train_max)
            if self._stop.wait(delay):
                return
            try:
                self._spawn_train()
            except Exception:
                log.exception("train spawn failed")

    # --- surge / auto-surge -----------------------------------------------

    def trigger_surge(self, factor: float | None = None, duration: float | None = None) -> None:
        self.state.start_surge(
            factor or self.config.surge_factor,
            duration or self.config.surge_duration,
        )
        log.info("SURGE x%.1f for %ss", factor or self.config.surge_factor, duration or self.config.surge_duration)

    def trigger_reset(self) -> None:
        self.state.reset()
        log.info("RESET: surge cleared, occupancy returned to mid-band")

    def _auto_surge_loop(self) -> None:
        interval = self.config.auto_surge_after
        while not self._stop.is_set():
            if self._stop.wait(interval):
                return
            self.trigger_surge()

    # --- lifecycle --------------------------------------------------------

    def _emit_initial_controls(self) -> None:
        """Publish the starting control state so the dashboard shows it at once."""
        self.producer.send(
            "gateline_state",
            producers.gateline_record(
                self.config.station_name, "OPEN", ref.THROTTLE_OPEN, "Gateline open"
            ),
        )
        for side in (ref.SIDE_LEFT, ref.SIDE_RIGHT):
            self.producer.send(
                "signal_state",
                producers.signal_record(
                    self.config.station_name, side, "GREEN", "Platform clear", self.state.occupancy
                ),
            )

    def start(self, with_control_consumers: bool = True) -> None:
        self._emit_heartbeat()
        self._emit_initial_controls()
        self._start_thread(self._foot_loop, "foot-loop")
        self._start_thread(self._train_spawn_loop, "train-spawn")
        if self.config.auto_surge_after > 0:
            self._start_thread(self._auto_surge_loop, "auto-surge")
        if with_control_consumers and self.config.kafka_configured:
            from .control_consumers import ControlConsumers

            # The emulator now OWNS signals + gateline (deterministic); the only
            # control it consumes is the presenter's surge/reset (demo_control).
            self._control = ControlConsumers(self.config, self.state)
            self._control.start()

    def _start_thread(self, target, name) -> None:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        self._threads.append(t)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=3)
        if getattr(self, "_control", None):
            self._control.stop()
        self.producer.flush(5)

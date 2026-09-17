"""Threaded emulator orchestrator.

Owns the foot loop, the train spawn loop + per-train state machine, and (when
Kafka is configured) the three control consumer threads that close the loop by
updating StationState from signal_state / gateline_state / demo_control.

The per-step methods (``foot_tick``, ``emit_approach``, ``arrive``, ``depart``)
are side-effect-scoped and reused by the fast simulator and the tests, so the
lifecycle logic is exercised without real sleeps where possible.
"""

from __future__ import annotations

import logging
import random
import threading
import uuid

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
        # AI-directed relief train, armed once per critical episode. Armed at
        # start; disarmed on dispatch; re-armed when occupancy drains back below
        # PCT_LOW (the episode is over). Guards against acting on repeated or
        # stale directives while the station is still critical.
        self._relief_armed = True
        self._relief_lock = threading.Lock()

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
        """One foot-flow tick: admit street inflow, emit flow (if any) + heartbeat."""
        base = self.rng.randint(self.config.foot_min, self.config.foot_max)
        # Local safety net: never admit street inflow at/above CRITICAL occupancy,
        # even if the gateline control loop is stalled/stale. Flink's gateline is
        # still the primary control; this only prevents a runaway if it goes quiet.
        if self.state.occupancy_pct() >= ref.PCT_CRITICAL:
            entered = 0
        else:
            entered = self.state.admit_foot(base)
        if entered > 0:
            self.producer.send(
                "passengers_flow",
                producers.foot_record(self.config.station_name, entered),
            )
        # Always heartbeat so the control loops keep seeing occupancy even when
        # the gateline is CLOSED (entered == 0) — needed to reopen it later.
        self._emit_heartbeat()
        # Re-arm the relief train once the crowd has drained back to normal, so
        # the next critical episode can dispatch a fresh reserve.
        if self.state.occupancy_pct() < ref.PCT_LOW:
            with self._relief_lock:
                self._relief_armed = True
        return entered

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

    def new_relief_spec(self) -> TrainSpec:
        """An empty, full-capacity reserve train dispatched at critical occupancy.

        Arrives empty (no alighting), so its whole capacity is spare for boarding
        people out, reaches the platform fast, and is flagged ``relief`` so it
        bypasses the signals (control clears the road for the reserve).
        """
        direction = self.rng.choice(ref.DIRECTIONS)
        return TrainSpec(
            service_id="relief-" + uuid.uuid4().hex[:6],
            direction=direction,
            train_type=ref.RELIEF_TRAIN_TYPE,
            capacity=ref.RELIEF_TRAIN_CAPACITY,
            passengers=0,                       # empty reserve
            side=ref.entry_side(direction),
            eta=self.config.relief_eta,
            staying=0,
            relief=True,
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
        if spec.get("relief"):
            # A relief train is dedicated to draining the crowd: fill every spare
            # seat (capped by occupancy and spare inside board_out), rather than
            # the fractional self-stabilising demand a normal service takes.
            desired = self.state.occupancy
        else:
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

    def _wait_for_green(self, side: str) -> bool:
        """Hold at a RED signal, re-polling. Returns True if released, False if stopped.

        Deadlock-breaker safety net: at/above CRITICAL occupancy, proceed even if
        the signal is (a possibly stale) RED — trains are the only drain, so they
        must never be held when the station is overfull. Flink's signal loop is
        still the primary control; this only rescues a stalled/stale RED.
        """
        while self.state.is_red(side) and self.state.occupancy_pct() < ref.PCT_CRITICAL:
            if self._stop.wait(self.config.signal_poll_interval):
                return False
        return True

    def _train_task(self, spec: TrainSpec) -> None:
        try:
            self.emit_approach(spec)
            if self._stop.wait(spec["eta"]):
                return
            # A relief train bypasses the signals — it is an emergency reserve the
            # controller clears straight to the platform to board people out.
            if not spec.get("relief") and not self._wait_for_green(spec["side"]):
                return  # stopped while held at RED
            self.arrive(spec)
            dwell = self.rng.randint(self.config.dwell_min, self.config.dwell_max)
            if self._stop.wait(dwell):
                return
            self.depart(spec)
        except Exception:
            log.exception("train task failed")
        finally:
            with self._trains_lock:
                self._trains.discard(threading.current_thread())

    def _spawn_train(self, spec: TrainSpec | None = None) -> None:
        spec = spec or self.new_train_spec()
        t = threading.Thread(target=self._train_task, args=(spec,), name=f"train-{spec['service_id']}", daemon=True)
        with self._trains_lock:
            self._trains.add(t)
        t.start()

    def dispatch_relief_train(self) -> bool:
        """AI-directed: bring the standby reserve into service to drain the crowd.

        Called from the AI-suggestion consumer when the advisor emits the
        ``DISPATCH_RELIEF_TRAIN`` directive. Fires at most once per critical
        episode (the latch is re-armed once occupancy drains below PCT_LOW), and
        only while the station is genuinely busy (>= PCT_HIGH), so a stale
        directive arriving after recovery is ignored. Returns True if dispatched.
        """
        with self._relief_lock:
            if not self._relief_armed or self.state.occupancy_pct() < ref.PCT_HIGH:
                return False
            self._relief_armed = False
        log.info(
            "AI directive: dispatching relief train (occupancy %.0f%%)",
            self.state.occupancy_pct() * 100,
        )
        self._spawn_train(self.new_relief_spec())
        return True

    def divert_to_street(self) -> int:
        """AI-directed: let waiting passengers leave via the street.

        Actuates the advisor recommending alternatives / buses / other stations
        (toolkit item 6). A batch of the current crowd leaves on foot, emitted as
        a NEGATIVE passengers_flow so the WebUI shows people leaving. Only acts
        while the station is genuinely busy (>= PCT_HIGH); StationState.street_egress
        can never remove more than are present, so occupancy never goes negative.
        Returns the number who left (>= 0).
        """
        if self.state.occupancy_pct() < ref.PCT_HIGH:
            return 0
        want = round(self.state.occupancy * self.config.street_divert_fraction)
        leaving = self.state.street_egress(want)
        if leaving > 0:
            self.producer.send(
                "passengers_flow",
                producers.foot_record(self.config.station_name, -leaving),
            )
            log.info("AI directive: %d passengers left via the street (alternatives/buses)", leaving)
        self._emit_heartbeat()
        return leaving

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

    def start(self, with_control_consumers: bool = True) -> None:
        self._emit_heartbeat()
        self._start_thread(self._foot_loop, "foot-loop")
        self._start_thread(self._train_spawn_loop, "train-spawn")
        if self.config.auto_surge_after > 0:
            self._start_thread(self._auto_surge_loop, "auto-surge")
        if with_control_consumers and self.config.kafka_configured:
            from .control_consumers import ControlConsumers

            # Map each AI directive token to the emulator action it actuates. The
            # advisor appends these (stripped from the human-facing advice); adding
            # a new AI-driven action is just a new entry here + a prompt line.
            self._control = ControlConsumers(
                self.config, self.state,
                ai_directives={
                    ref.RELIEF_DIRECTIVE: self.dispatch_relief_train,
                    ref.DIVERT_DIRECTIVE: self.divert_to_street,
                },
            )
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

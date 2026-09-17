"""Fast virtual-clock simulation of the emulator core.

Drives the *same* StationState + passenger model + record builders as the real
threaded Emulator, but on a virtual clock with no real sleeps, so a multi-hour
run completes in milliseconds. Used to prove:

* the occupancy invariant (never negative) and load conservation over a long
  randomised run;
* self-stabilisation (occupancy holds a mid-band in normal mode);
* a presenter surge pushes occupancy up and it recovers after.

Signals are assumed GREEN here (control-loop obedience — holding at RED — is
tested against the threaded Emulator instead).
"""

from __future__ import annotations

import heapq
import itertools
import random
from dataclasses import dataclass, field

from . import model, producers, reference as ref
from .config import EmulatorConfig
from .producers import EventProducer, InMemoryEventProducer
from .state import StationState


@dataclass
class SimResult:
    samples: list[tuple[float, int]] = field(default_factory=list)  # (vtime, occupancy)
    producer: EventProducer | None = None
    state: StationState | None = None

    @property
    def occupancies(self) -> list[int]:
        return [occ for _, occ in self.samples]


class _VirtualClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class Simulator:
    def __init__(
        self,
        config: EmulatorConfig,
        rng: random.Random | None = None,
        producer: EventProducer | None = None,
    ):
        self.config = config
        self.rng = rng or random.Random(config.frontend_seed)
        self.clock = _VirtualClock()
        self.state = StationState(
            capacity=config.capacity,
            initial_occupancy=config.initial_occupancy,
            clock=self.clock,
        )
        self.producer = producer or InMemoryEventProducer(validate=False)
        self._heap: list[tuple[float, int, object]] = []
        self._seq = itertools.count()
        # bookkeeping for assertions
        self.min_occupancy = self.state.occupancy
        self.max_occupancy = self.state.occupancy
        self.conservation_ok = True

    def _at(self, vtime: float, fn) -> None:
        heapq.heappush(self._heap, (vtime, next(self._seq), fn))

    def _emit_heartbeat(self) -> None:
        self.producer.send(
            "station_occupancy",
            producers.occupancy_record(self.config.station_name, self.state.occupancy, self.config.capacity),
        )
        self._track()

    def _track(self) -> None:
        occ = self.state.occupancy
        self.min_occupancy = min(self.min_occupancy, occ)
        self.max_occupancy = max(self.max_occupancy, occ)

    # --- scheduled events --------------------------------------------------

    def _foot(self) -> None:
        base = self.rng.randint(self.config.foot_min, self.config.foot_max)
        entered = self.state.admit_foot(base)
        if entered > 0:
            self.producer.send("passengers_flow", producers.foot_record(self.config.station_name, entered))
        self._emit_heartbeat()
        self._at(self.clock.t + self.config.foot_interval, self._foot)

    def _spawn(self) -> None:
        direction = self.rng.choice(ref.DIRECTIONS)
        train_type, cap = model.pick_train(self.rng)
        p = model.arriving_load(self.rng, cap, self.state.surge_active())
        eta = self.rng.randint(self.config.eta_min, self.config.eta_max)
        sid = format(next(self._seq), "08x")
        spec = {"sid": sid, "direction": direction, "type": train_type, "cap": cap, "p": p, "staying": 0}
        self.producer.send(
            "train_in_transit",
            producers.approach_record(sid, direction, p, train_type, cap, eta),
        )
        self._emit_heartbeat()
        self._at(self.clock.t + eta, lambda: self._arrive(spec))
        # next spawn
        self._at(self.clock.t + self.rng.randint(self.config.train_min, self.config.train_max), self._spawn)

    def _arrive(self, spec: dict) -> None:
        split = model.split_arriving_load(spec["p"], self.rng)
        if split.total != spec["p"]:
            self.conservation_ok = False
        spec["staying"] = split.staying
        self.state.alight_to_wait(split.station)
        self.producer.send(
            "train_in_station",
            producers.in_station_record(spec["sid"], spec["direction"], split.staying, split.out, split.station, spec["type"]),
        )
        self._emit_heartbeat()
        dwell = self.rng.randint(self.config.dwell_min, self.config.dwell_max)
        self._at(self.clock.t + dwell, lambda: self._depart(spec))

    def _depart(self, spec: dict) -> None:
        desired = model.desired_boarding(self.state.occupancy, self.config.board_fraction)
        spare = spec["cap"] - spec["staying"]
        boarding = self.state.board_out(desired, spare)
        # departing.passengers = staying + boarding; boarding <= spare and <= occupancy(before)
        if boarding > spare or boarding < 0:
            self.conservation_ok = False
        self.producer.send(
            "train_in_transit",
            producers.depart_record(spec["sid"], spec["direction"], spec["staying"], boarding, spec["type"], spec["cap"]),
        )
        self._emit_heartbeat()

    def _sample(self, interval: float, result: SimResult) -> None:
        result.samples.append((self.clock.t, self.state.occupancy))
        self._at(self.clock.t + interval, lambda: self._sample(interval, result))

    # --- run ---------------------------------------------------------------

    def run(self, duration_s: float, sample_interval: float = 5.0) -> SimResult:
        result = SimResult(producer=self.producer, state=self.state)
        self._at(0.0, self._foot)
        self._at(self.rng.randint(self.config.train_min, self.config.train_max), self._spawn)
        self._at(0.0, lambda: self._sample(sample_interval, result))

        while self._heap:
            vtime, _, fn = self._heap[0]
            if vtime > duration_s:
                break
            heapq.heappop(self._heap)
            self.clock.t = vtime
            fn()
            self._track()
            if self.state.occupancy < 0:  # should be impossible
                raise AssertionError("occupancy went negative")

        return result

    def schedule_surge(self, at: float, factor: float, duration: float) -> None:
        """Schedule a presenter surge at virtual time ``at`` (call before run)."""
        self._at(at, lambda: self.state.start_surge(factor, duration))

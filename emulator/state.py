"""Thread-safe station state — the single source of truth.

Occupancy lives here and only here. Non-negativity is guaranteed at the source:
``board_out`` can never remove more than the current occupancy, so occupancy is
mathematically incapable of going negative — never enforced downstream in SQL.
"""

from __future__ import annotations

import threading
import time
from typing import Callable

from . import reference as ref


class StationState:
    def __init__(
        self,
        capacity: int,
        evacuation_flow: float,
        initial_occupancy: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._lock = threading.RLock()
        self._capacity = capacity
        self._evac_flow = evacuation_flow
        self._initial_occupancy = max(0, initial_occupancy)
        self._occupancy = self._initial_occupancy
        self._clock = clock

        # Latest control state received from Flink (via consumer threads).
        self._signal = {ref.SIDE_LEFT: "GREEN", ref.SIDE_RIGHT: "GREEN"}
        self._throttle = 1.0

        # Surge (presenter-controlled).
        self._surge_factor = 1.0
        self._surge_expiry = 0.0

    # --- occupancy (invariant core) ---------------------------------------

    @property
    def occupancy(self) -> int:
        with self._lock:
            return self._occupancy

    @property
    def capacity(self) -> int:
        return self._capacity

    def occupancy_pct(self) -> float:
        with self._lock:
            return self._occupancy / self._capacity if self._capacity else 0.0

    def evac_time(self) -> float:
        with self._lock:
            return self._occupancy / self._evac_flow if self._evac_flow else 0.0

    def admit_foot(self, base: int) -> int:
        """Admit street foot flow: entered = round(base * surge * throttle).

        Adds to occupancy and returns the number actually admitted (>= 0).
        Applies the live surge factor and gateline throttle atomically.
        """
        with self._lock:
            raw = base * self._effective_surge_factor()
            entered = round(raw * self._throttle)
            if entered < 0:
                entered = 0
            self._occupancy += entered
            return entered

    def alight_to_wait(self, station: int) -> None:
        """Passengers alighting to wait join occupancy."""
        if station <= 0:
            return
        with self._lock:
            self._occupancy += station

    def board_out(self, desired: int, train_spare: int) -> int:
        """Board waiting passengers onto a departing train (drains occupancy).

        boarding = min(desired, occupancy, train_spare); occupancy -= boarding.
        Returns boarding (>= 0). Guarantees occupancy never goes negative.
        """
        with self._lock:
            boarding = min(max(0, desired), self._occupancy, max(0, train_spare))
            self._occupancy -= boarding
            return boarding

    # --- signals ----------------------------------------------------------

    def set_signal(self, side: str, state: str) -> None:
        with self._lock:
            self._signal[side] = state

    def get_signal(self, side: str) -> str:
        with self._lock:
            return self._signal[side]

    def is_red(self, side: str) -> bool:
        with self._lock:
            return self._signal[side] == "RED"

    # --- gateline throttle ------------------------------------------------

    def set_throttle(self, factor: float) -> None:
        with self._lock:
            self._throttle = max(0.0, float(factor))

    @property
    def throttle(self) -> float:
        with self._lock:
            return self._throttle

    # --- surge ------------------------------------------------------------

    def _effective_surge_factor(self) -> float:
        # caller holds the lock
        if self._clock() < self._surge_expiry:
            return self._surge_factor
        return 1.0

    @property
    def surge_factor(self) -> float:
        with self._lock:
            return self._effective_surge_factor()

    def surge_active(self) -> bool:
        with self._lock:
            return self._clock() < self._surge_expiry

    def start_surge(self, factor: float, duration_seconds: float) -> None:
        with self._lock:
            self._surge_factor = float(factor)
            self._surge_expiry = self._clock() + float(duration_seconds)

    def reset(self) -> None:
        """Return to a clean steady state.

        Clears the surge and returns occupancy to the normal mid-band, and opens
        the gateline (throttle 1.0) so foot inflow can resume immediately; the
        emulator re-derives the gateline band on the next tick. Signals are left
        untouched — they are the emulator's block-signal state (a train may still
        be dwelling), and it publishes GREEN when the platform actually clears.
        """
        with self._lock:
            self._surge_factor = 1.0
            self._surge_expiry = 0.0
            self._occupancy = self._initial_occupancy
            self._throttle = 1.0

    # --- snapshot ---------------------------------------------------------

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "occupancy": self._occupancy,
                "capacity": self._capacity,
                "occupancy_pct": self._occupancy / self._capacity if self._capacity else 0.0,
                "throttle": self._throttle,
                "signal": dict(self._signal),
                "surge_active": self._clock() < self._surge_expiry,
            }

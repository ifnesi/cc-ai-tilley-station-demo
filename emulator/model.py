"""Pure passenger-model functions.

No shared state, no side effects — the invariant-bearing arithmetic that the
StationState and tests both rely on.
"""

from __future__ import annotations

import random
from typing import NamedTuple

from . import reference as ref


class ArrivingSplit(NamedTuple):
    staying: int   # remain onboard — no occupancy effect
    out: int       # alight & exit to street — pass-through
    station: int   # alight & wait — joins occupancy

    @property
    def total(self) -> int:
        return self.staying + self.out + self.station


def split_arriving_load(
    p: int,
    rng: random.Random,
    stay_frac_range: tuple[float, float] = (0.2, 0.4),
    out_frac_range: tuple[float, float] = (0.4, 0.6),
) -> ArrivingSplit:
    """Split an arriving load P into (staying, out, station) with P conserved.

    Invariant: staying + out + station == p, and all >= 0.
    """
    if p <= 0:
        return ArrivingSplit(0, 0, 0)
    staying = round(p * rng.uniform(*stay_frac_range))
    staying = min(staying, p)
    remaining = p - staying
    out = round(remaining * rng.uniform(*out_frac_range))
    out = min(out, remaining)
    station = remaining - out
    return ArrivingSplit(staying=staying, out=out, station=station)


def desired_boarding(occupancy: int, board_fraction: float) -> int:
    """Self-stabilising boarding demand ∝ occupancy.

    Negative feedback that holds occupancy in a mid-band without per-rate tuning.
    Caller still caps by current occupancy and the train's spare capacity.
    """
    return round(max(0, occupancy) * board_fraction)


def pick_train(rng: random.Random) -> tuple[str, int]:
    """Random (train_type, train_capacity)."""
    train_type = rng.choice(ref.TRAIN_TYPES)
    return train_type, ref.train_capacity(train_type)


def arriving_load(
    rng: random.Random,
    capacity: int,
    surge_active: bool,
    low_frac: float = 0.3,
    hi_frac_normal: float = 0.7,
) -> int:
    """Onboard load P for an approaching train, P <= capacity.

    A surge biases toward fuller trains.
    """
    lo = int(capacity * low_frac)
    hi = capacity if surge_active else int(capacity * hi_frac_normal)
    hi = max(hi, lo + 1)
    return min(rng.randint(lo, hi), capacity)

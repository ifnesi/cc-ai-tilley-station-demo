"""Static reference data for the Tilley Station demo.

Single source of truth for capacities, labels and the direction/geography
convention. Shared by the emulator (enforces rules), the backend, the mock
feed, and — mirrored — by the frontend and Flink SQL constants.

Geography (west -> east): Overton -- Tilley -- Jones.
Only Tilley is modelled for crowding; the neighbours are visual endpoints.
"""

from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

# --- Shared domain constants: from the environment only -------------------
# NOTHING is hard-coded here. Values come from the root .env (copied from
# .env_example and written by `terraform apply` from terraform/vars.tf); in
# Docker they arrive via env_file so os.environ is already populated. If a value
# is missing the app fails loudly (KeyError) rather than using a silent default.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env")

_CONFIG = {
    "station_name": os.environ["STATION_NAME"],
    "station_capacity": int(os.environ["STATION_CAPACITY"]),
    "agg_window_seconds": int(os.environ["AGG_WINDOW_SECONDS"]),
    "pct_low": float(os.environ["PCT_LOW"]),
    "pct_high": float(os.environ["PCT_HIGH"]),
    "pct_critical": float(os.environ["PCT_CRITICAL"]),
}

# --- station_data (one modelled station) -------------------------------

STATION_DATA: dict[str, object] = {
    "station_name": _CONFIG["station_name"],
    "capacity": int(_CONFIG["station_capacity"]),  # max passengers inside (excludes staff)
}

# --- train_data (four classes) -----------------------------------------

TRAIN_DATA: dict[str, dict[str, int]] = {
    "A72": {"capacity": 200},
    "B85": {"capacity": 225},
    "C98": {"capacity": 250},
    "D11": {"capacity": 275},
}

TRAIN_TYPES: list[str] = list(TRAIN_DATA)

# --- Deterministic gateline throttle bands (emulator-controlled) -------
# The emulator sets the street gateline directly from its own occupancy — a
# deterministic operational rule, no Flink/AI needed. throttle multiplies the
# street foot inflow: OPEN=1.0, RESTRICTED=0.4, CLOSED=0.0.
THROTTLE_OPEN = 1.0
THROTTLE_RESTRICTED = 0.4
THROTTLE_CLOSED = 0.0

# --- Geography & direction convention --------------------------------

NEIGHBOUR_WEST = "Overton Station"  # left endpoint
NEIGHBOUR_EAST = "Jones Station"    # right endpoint

EAST_BOUND = "east_bound"
WEST_BOUND = "west_bound"
DIRECTIONS: tuple[str, str] = (EAST_BOUND, WEST_BOUND)

SIDE_LEFT = "LEFT"
SIDE_RIGHT = "RIGHT"

# east_bound travels west->east (Overton -> Tilley -> Jones), enters via the
# LEFT signal, and leaves toward Jones. west_bound is the mirror image.
DIRECTION_META: dict[str, dict[str, str]] = {
    EAST_BOUND: {
        "entry_side": SIDE_LEFT,
        "arrives_from": NEIGHBOUR_WEST,
        "departs_to": NEIGHBOUR_EAST,
    },
    WEST_BOUND: {
        "entry_side": SIDE_RIGHT,
        "arrives_from": NEIGHBOUR_EAST,
        "departs_to": NEIGHBOUR_WEST,
    },
}

STATION_NAME: str = STATION_DATA["station_name"]  # type: ignore[assignment]
STATION_CAPACITY: int = STATION_DATA["capacity"]  # type: ignore[assignment]

# Windowing + control thresholds (from .env; Flink uses the same values from
# terraform/vars.tf). Documented in .env_example.
AGG_WINDOW_SECONDS: int = int(_CONFIG["agg_window_seconds"])
PCT_LOW: float = float(_CONFIG["pct_low"])
PCT_HIGH: float = float(_CONFIG["pct_high"])
PCT_CRITICAL: float = float(_CONFIG["pct_critical"])


def train_capacity(train_type: str) -> int:
    """Capacity for a train class, e.g. train_capacity('C98') -> 250."""
    return TRAIN_DATA[train_type]["capacity"]


def entry_side(direction: str) -> str:
    """Signal side that gates arrivals for a direction ('LEFT'/'RIGHT')."""
    return DIRECTION_META[direction]["entry_side"]


def departs_to(direction: str) -> str:
    """Neighbour a departing train of this direction heads toward."""
    return DIRECTION_META[direction]["departs_to"]


def arrives_from(direction: str) -> str:
    """Neighbour a train of this direction approaches Tilley from."""
    return DIRECTION_META[direction]["arrives_from"]

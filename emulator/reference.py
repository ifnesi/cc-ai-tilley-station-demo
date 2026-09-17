"""Static reference data for the Tilley Station demo.

Single source of truth for capacities, labels and the direction/geography
convention. Shared by the emulator (enforces rules), the backend, the mock
feed, and — mirrored — by the frontend and Flink SQL constants.

Geography (west -> east): Overton -- Tilley -- Jones.
Only Tilley is modelled for crowding; the neighbours are visual endpoints.
"""

from __future__ import annotations

import json
import pathlib

# --- Shared domain constants: single source of truth ----------------------
# demo.config.json (repo root) is read by BOTH Terraform and Python so the
# station/window/threshold values are defined once. Fallback defaults keep this
# import working even if the file is missing (e.g. isolated unit runs).

_CONFIG_PATH = pathlib.Path(__file__).resolve().parents[1] / "demo.config.json"
_DEFAULTS: dict[str, object] = {
    "station_name": "Tilley Station",
    "station_capacity": 1200,
    "evacuation_flow": 3.5,
    "agg_window_seconds": 15,
    "pct_low": 0.70,
    "pct_high": 0.85,
    "pct_critical": 0.95,
    "evac_warn": 360,
    "evac_crit": 480,
}


def _load_config() -> dict[str, object]:
    try:
        data = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return dict(_DEFAULTS)
    # ignore the _comment key and any unknown extras; fall back per-key
    return {k: data.get(k, v) for k, v in _DEFAULTS.items()}


_CONFIG = _load_config()

# --- station_data (one modelled station) -------------------------------

STATION_DATA: dict[str, object] = {
    "station_name": _CONFIG["station_name"],
    "capacity": int(_CONFIG["station_capacity"]),  # max passengers inside (excludes staff)
    "evacuation_flow": float(_CONFIG["evacuation_flow"]),  # passengers/second to evacuate
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
EVACUATION_FLOW: float = STATION_DATA["evacuation_flow"]  # type: ignore[assignment]

# Windowing + control thresholds (Flink's domain; exposed here so any Python
# that needs them reads the same single source). See demo.config.json.
AGG_WINDOW_SECONDS: int = int(_CONFIG["agg_window_seconds"])
PCT_LOW: float = float(_CONFIG["pct_low"])
PCT_HIGH: float = float(_CONFIG["pct_high"])
PCT_CRITICAL: float = float(_CONFIG["pct_critical"])
EVAC_WARN: float = float(_CONFIG["evac_warn"])
EVAC_CRIT: float = float(_CONFIG["evac_crit"])


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

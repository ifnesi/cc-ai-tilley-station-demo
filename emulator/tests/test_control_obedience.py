"""Deterministic control stop gate.

The emulator now OWNS the operational controls (no Flink/AI in the loop):

* Gateline — throttles street inflow straight from occupancy bands, published on
  change; foot inflow stops when CLOSED.
* Signals — block signalling: a train holds the platform (signal RED) while it
  dwells; the next train on that side queues until the platform frees (GREEN).
* demo_control — the presenter's SURGE / RESET is still consumed.
"""

from __future__ import annotations

import threading

from emulator import reference as ref
from emulator.config import EmulatorConfig
from emulator.control_consumers import ControlConsumers
from emulator.emulator import Emulator, TrainSpec
from emulator.producers import InMemoryEventProducer
from emulator.state import StationState


def make_emulator(occupancy: int | None = None, **overrides):
    cfg = EmulatorConfig(
        eta_min=0, eta_max=0, dwell_min=0, dwell_max=0,
        foot_interval=0.02, **overrides,
    )
    occ = cfg.initial_occupancy if occupancy is None else occupancy
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow,
                         initial_occupancy=occ)
    return Emulator(cfg, state, InMemoryEventProducer(validate=True)), state, cfg


def _train(sid, side=ref.SIDE_LEFT, direction=ref.EAST_BOUND):
    return TrainSpec(service_id=sid, direction=direction, train_type="C98",
                     capacity=250, passengers=120, side=side, eta=0, staying=0)


# --- gateline (deterministic bands) ---------------------------------------

def test_gateline_bands_emit_and_throttle():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.50))  # OPEN
    emu.foot_tick()
    assert state.throttle == ref.THROTTLE_OPEN

    state.alight_to_wait(int(1200 * 0.90) - state.occupancy)   # -> HIGH band
    emu.foot_tick()
    assert state.throttle == ref.THROTTLE_RESTRICTED
    assert emu.producer.by_topic("gateline_state")[-1]["state"] == "RESTRICTED"

    state.alight_to_wait(int(1200 * 0.97) - state.occupancy)   # -> CRITICAL band
    entered = emu.foot_tick()
    assert state.throttle == ref.THROTTLE_CLOSED
    assert entered == 0                                          # gateline closed
    assert emu.producer.by_topic("gateline_state")[-1]["state"] == "CLOSED"


def test_gateline_reopens_when_crowd_drains():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.97))
    emu.foot_tick()
    assert emu.producer.by_topic("gateline_state")[-1]["state"] == "CLOSED"
    state.board_out(state.occupancy - int(1200 * 0.40), 10_000)  # drain to 40%
    emu.foot_tick()
    assert emu.producer.by_topic("gateline_state")[-1]["state"] == "OPEN"
    assert emu.foot_tick() > 0  # inflow resumes


# --- block signalling ------------------------------------------------------

def test_train_sets_red_then_green_around_dwell():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.50))
    emu._train_task(_train("t1"))  # runs to completion (eta/dwell 0)

    left = [r["state"] for r in emu.producer.by_topic("signal_state") if r["side"] == "LEFT"]
    assert "RED" in left and left[-1] == "GREEN"     # RED on arrival, GREEN on release
    assert state.get_signal(ref.SIDE_LEFT) == "GREEN"
    assert any(r["service_id"] == "t1" for r in emu.producer.by_topic("train_in_station"))


def test_second_train_queues_until_platform_frees():
    emu, _, _ = make_emulator(occupancy=int(1200 * 0.50))
    a, b = _train("a"), _train("b")  # same side

    assert emu._acquire_platform(a) is True          # A takes the platform (RED)
    result = {}
    th = threading.Thread(target=lambda: result.__setitem__("b", emu._acquire_platform(b)))
    th.start()
    th.join(timeout=0.4)
    assert th.is_alive()                             # B is held behind the signal

    emu._release_platform(a)                          # A departs, platform frees
    th.join(timeout=1.0)
    assert not th.is_alive() and result["b"] is True  # B now advances
    emu._release_platform(b)


def test_opposite_sides_do_not_block_each_other():
    emu, _, _ = make_emulator(occupancy=int(1200 * 0.50))
    left = _train("L", side=ref.SIDE_LEFT, direction=ref.EAST_BOUND)
    right = _train("R", side=ref.SIDE_RIGHT, direction=ref.WEST_BOUND)
    assert emu._acquire_platform(left) is True
    assert emu._acquire_platform(right) is True       # different platform, no wait
    emu._release_platform(left)
    emu._release_platform(right)


# --- presenter control -----------------------------------------------------

def test_demo_control_surge_and_reset():
    cfg = EmulatorConfig()
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow,
                         initial_occupancy=cfg.initial_occupancy)
    control = ControlConsumers(cfg, state)

    control._on_demo_control({"command": "SURGE", "factor": 5.0, "duration_seconds": 30})
    assert state.surge_active() is True and state.surge_factor == 5.0

    state.alight_to_wait(200)
    control._on_demo_control({"command": "RESET", "factor": None, "duration_seconds": None})
    assert state.surge_active() is False
    assert state.occupancy == cfg.initial_occupancy

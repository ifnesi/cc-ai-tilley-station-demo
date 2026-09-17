"""Phase-1 control-obedience stop gate.

Injects control state directly into StationState (no broker) and asserts the
threaded emulator obeys: trains hold at RED, throttle=0 stops foot inflow, and
SURGE/RESET (via the demo_control handler) change behaviour as specified.
"""

from __future__ import annotations

import threading
import time

from emulator import reference as ref
from emulator.config import EmulatorConfig
from emulator.control_consumers import ControlConsumers
from emulator.emulator import Emulator, TrainSpec
from emulator.producers import InMemoryEventProducer
from emulator.state import StationState


def make_emulator(**overrides):
    cfg = EmulatorConfig(
        eta_min=0, eta_max=0, dwell_min=0, dwell_max=0,
        signal_poll_interval=0.02, foot_interval=0.02, **overrides,
    )
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow,
                         initial_occupancy=cfg.initial_occupancy)
    producer = InMemoryEventProducer(validate=True)
    return Emulator(cfg, state, producer), state, producer


def _in_station_for(producer, sid):
    return [r for r in producer.by_topic("train_in_station") if r["service_id"] == sid]


def test_train_holds_at_red_until_green():
    emu, state, producer = make_emulator()
    sid = "deadbeef"
    spec = TrainSpec(service_id=sid, direction=ref.EAST_BOUND, train_type="C98",
                     capacity=250, passengers=120, side=ref.SIDE_LEFT, eta=0, staying=0)

    state.set_signal(ref.SIDE_LEFT, "RED")
    th = threading.Thread(target=emu._train_task, args=(spec,), daemon=True)
    th.start()

    time.sleep(0.2)
    # approach emitted, but held at RED — no train_in_station yet
    assert any(r["service_id"] == sid for r in producer.by_topic("train_in_transit"))
    assert _in_station_for(producer, sid) == []

    state.set_signal(ref.SIDE_LEFT, "GREEN")
    th.join(timeout=2)
    assert not th.is_alive()
    # now it proceeds through the platform and departs
    assert len(_in_station_for(producer, sid)) == 1
    departs = [r for r in producer.by_topic("train_in_transit")
               if r["service_id"] == sid and r["next_stop"] != ref.STATION_NAME]
    assert len(departs) == 1


def test_throttle_zero_stops_foot_inflow():
    emu, state, producer = make_emulator()
    state.set_throttle(0.0)
    before = state.occupancy
    for _ in range(50):
        assert emu.foot_tick() == 0
    assert state.occupancy == before
    assert producer.count("passengers_flow") == 0
    # heartbeats still flow so the gateline can later reopen
    assert producer.count("station_occupancy") >= 50

    state.set_throttle(1.0)
    entered_total = sum(emu.foot_tick() for _ in range(50))
    assert entered_total > 0
    assert producer.count("passengers_flow") > 0


def test_demo_control_surge_and_reset():
    cfg = EmulatorConfig()
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow,
                         initial_occupancy=cfg.initial_occupancy)
    control = ControlConsumers(cfg, state)

    control._on_demo_control({"command": "SURGE", "factor": 5.0, "duration_seconds": 30})
    assert state.surge_active() is True
    assert state.surge_factor == 5.0

    state.alight_to_wait(200)
    control._on_demo_control({"command": "RESET", "factor": None, "duration_seconds": None})
    assert state.surge_active() is False
    assert state.occupancy == cfg.initial_occupancy


def test_control_handlers_update_signal_and_throttle():
    cfg = EmulatorConfig()
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow)
    control = ControlConsumers(cfg, state)

    control._on_signal({"side": ref.SIDE_RIGHT, "state": "RED"})
    assert state.is_red(ref.SIDE_RIGHT)
    control._on_gateline({"throttle_factor": 0.4})
    assert state.throttle == 0.4

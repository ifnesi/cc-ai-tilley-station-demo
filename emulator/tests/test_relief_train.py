"""AI-directed relief train stop gate.

At critical occupancy the AI advisor emits the DISPATCH_RELIEF_TRAIN directive;
the emulator parses it off station_ai_suggestions and dispatches an empty,
full-capacity reserve train that bypasses the signals and drains the crowd. It
fires at most once per critical episode (re-armed when occupancy drains back to
normal) and is ignored when the station is not actually busy.
"""

from __future__ import annotations

import threading

from emulator import reference as ref
from emulator.config import EmulatorConfig
from emulator.control_consumers import ControlConsumers
from emulator.emulator import Emulator
from emulator.producers import InMemoryEventProducer
from emulator.state import StationState


def make_emulator(occupancy: int, **overrides):
    cfg = EmulatorConfig(
        eta_min=0, eta_max=0, dwell_min=0, dwell_max=0, relief_eta=0,
        signal_poll_interval=0.02, foot_interval=0.02, **overrides,
    )
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow,
                         initial_occupancy=occupancy)
    return Emulator(cfg, state, InMemoryEventProducer(validate=True)), state, cfg


def _run_relief(emu):
    """Dispatch and run one relief train task to completion (synchronously)."""
    spec = emu.new_relief_spec()
    emu._train_task(spec)
    return spec


def test_relief_train_drains_occupancy():
    critical = int(1200 * 0.96)  # 1152, above PCT_CRITICAL
    emu, state, _ = make_emulator(occupancy=critical)
    before = state.occupancy

    spec = _run_relief(emu)

    boarded = min(before, ref.RELIEF_TRAIN_CAPACITY)
    assert state.occupancy == before - boarded
    departs = [r for r in emu.producer.by_topic("train_in_transit")
               if r["service_id"] == spec["service_id"] and r["next_stop"] != ref.STATION_NAME]
    assert len(departs) == 1
    assert departs[0]["train_type"] == ref.RELIEF_TRAIN_TYPE
    assert departs[0]["passengers_boarding"] == boarded > 0


def test_relief_bypasses_red_signals():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.96))
    state.set_signal(ref.SIDE_LEFT, "RED")
    state.set_signal(ref.SIDE_RIGHT, "RED")

    spec = _run_relief(emu)  # would hang on a normal train; relief bypasses

    in_station = [r for r in emu.producer.by_topic("train_in_station")
                  if r["service_id"] == spec["service_id"]]
    assert len(in_station) == 1  # arrived despite RED


def test_relief_ignored_when_not_busy():
    # 45% occupancy is below PCT_HIGH — a stale directive must not act.
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.45))
    dispatched = []
    emu._spawn_train = lambda spec=None: dispatched.append(spec)

    assert emu.dispatch_relief_train() is False
    assert dispatched == []


def test_relief_once_per_critical_episode():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.96))
    dispatched = []
    emu._spawn_train = lambda spec=None: dispatched.append(spec)

    assert emu.dispatch_relief_train() is True          # first fires
    assert emu.dispatch_relief_train() is False         # latch disarmed
    assert len(dispatched) == 1

    # Drain below PCT_LOW and tick the foot loop to re-arm for the next episode.
    state.board_out(state.occupancy, ref.RELIEF_TRAIN_CAPACITY * 10)  # empty it
    assert state.occupancy_pct() < ref.PCT_LOW
    emu.foot_tick()                                      # re-arms the latch

    # Back to critical -> can dispatch again.
    state.alight_to_wait(int(1200 * 0.96))
    assert emu.dispatch_relief_train() is True
    assert len(dispatched) == 2


def test_ai_consumer_routes_each_directive():
    cfg = EmulatorConfig()
    state = StationState(capacity=cfg.capacity, evacuation_flow=cfg.evacuation_flow)
    calls = []
    control = ControlConsumers(cfg, state, ai_directives={
        ref.RELIEF_DIRECTIVE: lambda: calls.append("relief"),
        ref.DIVERT_DIRECTIVE: lambda: calls.append("divert"),
    })

    control._on_ai_suggestion({"suggestion": "SEVERITY: HIGH\nACTIONS: 1) hold.\nWATCH: pct."})
    assert calls == []  # no directive -> no action

    control._on_ai_suggestion({"suggestion": "ACTIONS: relief.\nDIRECTIVE: DISPATCH_RELIEF_TRAIN"})
    assert calls == ["relief"]

    # both directives in one suggestion -> both fire
    control._on_ai_suggestion({
        "suggestion": "ACTIONS: x.\nDIRECTIVE: DISPATCH_RELIEF_TRAIN\nDIRECTIVE: DIVERT_TO_STREET",
    })
    assert sorted(calls[1:]) == ["divert", "relief"]

    control._on_ai_suggestion({"metric": "occupancy_pct"})  # missing field -> safe no-op
    assert len(calls) == 3


def test_street_egress_never_goes_negative():
    state = StationState(capacity=1200, evacuation_flow=3.5, initial_occupancy=50)
    left = state.street_egress(200)  # ask for more than present
    assert left == 50
    assert state.occupancy == 0      # never negative
    assert state.street_egress(10) == 0  # nothing left to remove


def test_divert_to_street_emits_negative_flow_and_drains():
    busy = int(1200 * 0.90)  # above PCT_HIGH
    emu, state, cfg = make_emulator(occupancy=busy)
    before = state.occupancy

    left = emu.divert_to_street()

    assert left == round(before * cfg.street_divert_fraction) > 0
    assert state.occupancy == before - left
    flows = emu.producer.by_topic("passengers_flow")
    assert flows and flows[-1]["passengers"] == -left  # negative = egress


def test_divert_ignored_when_not_busy():
    emu, state, _ = make_emulator(occupancy=int(1200 * 0.45))  # below PCT_HIGH
    assert emu.divert_to_street() == 0
    assert emu.producer.count("passengers_flow") == 0

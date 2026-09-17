"""StationState invariant core."""

from __future__ import annotations

import random

from emulator import reference as ref
from emulator.state import StationState


def make_state(occ=0, cap=1200, evac=3.5, clock=None):
    return StationState(capacity=cap, evacuation_flow=evac, initial_occupancy=occ,
                        clock=clock or (lambda: 0.0))


def test_board_out_never_goes_negative():
    st = make_state(occ=10)
    boarding = st.board_out(desired=100, train_spare=100)  # want more than present
    assert boarding == 10
    assert st.occupancy == 0
    # boarding again from empty yields nothing
    assert st.board_out(50, 50) == 0
    assert st.occupancy == 0


def test_board_out_capped_by_spare():
    st = make_state(occ=500)
    boarding = st.board_out(desired=400, train_spare=120)
    assert boarding == 120
    assert st.occupancy == 380


def test_admit_foot_applies_throttle():
    st = make_state(occ=0)
    st.set_throttle(0.0)
    assert st.admit_foot(40) == 0  # gateline CLOSED stops inflow
    assert st.occupancy == 0
    st.set_throttle(0.4)
    entered = st.admit_foot(10)
    assert entered == 4
    assert st.occupancy == 4


def test_admit_foot_applies_surge():
    clock = {"t": 0.0}
    st = StationState(capacity=1200, evacuation_flow=3.5, initial_occupancy=0, clock=lambda: clock["t"])
    st.start_surge(factor=5.0, duration_seconds=10)
    assert st.admit_foot(10) == 50  # 10 * surge5 * throttle1
    clock["t"] = 20.0  # surge expired
    assert st.admit_foot(10) == 10


def test_reset_returns_to_midband_and_clears_surge():
    clock = {"t": 0.0}
    st = StationState(capacity=1000, evacuation_flow=3.5, initial_occupancy=450, clock=lambda: clock["t"])
    st.start_surge(5.0, 60)
    st.alight_to_wait(300)
    st.set_throttle(0.0)                 # gateline CLOSED during the surge
    st.set_signal(ref.SIDE_LEFT, "RED")
    assert st.occupancy == 750
    st.reset()
    assert st.occupancy == 450
    assert st.surge_active() is False
    # reset also restores the local control state so foot inflow can resume
    assert st.throttle == 1.0
    assert st.is_red(ref.SIDE_LEFT) is False


def test_signal_hold_flags():
    st = make_state()
    assert st.is_red(ref.SIDE_LEFT) is False
    st.set_signal(ref.SIDE_LEFT, "RED")
    assert st.is_red(ref.SIDE_LEFT) is True
    st.set_signal(ref.SIDE_LEFT, "GREEN")
    assert st.is_red(ref.SIDE_LEFT) is False


def test_occupancy_never_negative_under_random_ops():
    rng = random.Random(0)
    st = make_state(occ=100)
    for _ in range(20000):
        op = rng.random()
        if op < 0.4:
            st.admit_foot(rng.randint(0, 40))
        elif op < 0.7:
            st.alight_to_wait(rng.randint(0, 60))
        else:
            st.board_out(rng.randint(0, 300), rng.randint(0, 275))
        assert st.occupancy >= 0

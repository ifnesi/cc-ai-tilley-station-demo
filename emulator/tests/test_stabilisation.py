"""Phase-1 self-stabilisation stop gate.

A normal run holds occupancy in a stable mid-band (neither drained to 0 nor
pinned at capacity); a presenter surge pushes it up sharply and it recovers
afterwards.
"""

from __future__ import annotations

import random
import statistics

import pytest

from emulator.config import EmulatorConfig
from emulator.sim import Simulator


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_normal_run_holds_midband(seed):
    cfg = EmulatorConfig()
    sim = Simulator(cfg, rng=random.Random(seed))
    res = sim.run(duration_s=3600, sample_interval=5)  # 1 virtual hour

    occ = res.occupancies
    warm = occ[len(occ) // 5:]  # discard warm-up (first 20%)
    lo, hi, mean = min(warm), max(warm), statistics.mean(warm)

    assert lo > 0, "occupancy drained to zero"
    assert hi < cfg.capacity, "occupancy pinned at/over capacity in normal mode"
    # a sane mid-band, not hugging either rail
    assert 0.10 * cfg.capacity < mean < 0.70 * cfg.capacity
    # the band is reasonably tight (self-regulating, not drifting)
    assert (hi - lo) < 0.7 * cfg.capacity


@pytest.mark.parametrize("seed", [5, 6])
def test_surge_pushes_up_then_recovers(seed):
    cfg = EmulatorConfig()
    sim = Simulator(cfg, rng=random.Random(seed))
    sim.schedule_surge(at=600, factor=cfg.surge_factor, duration=cfg.surge_duration)
    res = sim.run(duration_s=1800, sample_interval=5)

    baseline = statistics.mean([o for t, o in res.samples if 400 <= t < 600])
    peak = max([o for t, o in res.samples if 600 <= t <= 780])
    recovery = statistics.mean([o for t, o in res.samples if t >= 1600])

    assert peak > baseline * 1.6, "surge did not raise occupancy"
    assert peak > 0.70 * cfg.capacity, "surge should drive occupancy toward critical"
    assert recovery < 0.6 * cfg.capacity, "did not recover after the surge"
    assert recovery < peak * 0.6

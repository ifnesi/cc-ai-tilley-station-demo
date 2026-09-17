"""Pure passenger-model invariants."""

from __future__ import annotations

import random

import pytest

from emulator import model, reference as ref


@pytest.mark.parametrize("p", [0, 1, 5, 37, 199, 275])
def test_split_conserves_load(p):
    rng = random.Random(p)
    for _ in range(200):
        s = model.split_arriving_load(p, rng)
        assert s.staying >= 0 and s.out >= 0 and s.station >= 0
        assert s.staying + s.out + s.station == p # P conserved


def test_desired_boarding_proportional_and_nonneg():
    assert model.desired_boarding(1000, 0.35) == 350
    assert model.desired_boarding(0, 0.35) == 0
    assert model.desired_boarding(-50, 0.35) == 0  # clamped


@pytest.mark.parametrize("surge", [False, True])
def test_arriving_load_within_capacity(surge):
    rng = random.Random(1)
    for train_type in ref.TRAIN_TYPES:
        cap = ref.train_capacity(train_type)
        for _ in range(100):
            p = model.arriving_load(rng, cap, surge)
            assert 0 <= p <= cap


def test_pick_train_returns_known_type():
    rng = random.Random(2)
    for _ in range(50):
        t, cap = model.pick_train(rng)
        assert t in ref.TRAIN_TYPES
        assert cap == ref.train_capacity(t)

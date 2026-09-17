"""Phase-1 invariant stop gate over a long randomised run.

* occupancy never negative;
* arriving load conserved: P = staying + out + station;
* departing.passengers = staying + boarding, and <= train_capacity;
* every emitted record validates against the Phase-0 schemas.
"""

from __future__ import annotations

import random

import pytest

from emulator.config import EmulatorConfig
from emulator.producers import InMemoryEventProducer
from emulator.sim import Simulator
from emulator.schemas import load_schema

STATION = "Tilley Station"


def _join_by_service(records: list[tuple[str, dict]]):
    """Group the three lifecycle events by service_id."""
    trains: dict[str, dict] = {}
    for topic, rec in records:
        sid = rec.get("service_id")
        if sid is None:
            continue
        t = trains.setdefault(sid, {})
        if topic == "train_in_station":
            t["in_station"] = rec
        elif topic == "train_in_transit":
            if rec["next_stop"] == STATION:
                t["approach"] = rec
            else:
                t["depart"] = rec
    return trains


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_long_run_invariants(seed):
    cfg = EmulatorConfig()
    sim = Simulator(cfg, rng=random.Random(seed))
    res = sim.run(duration_s=7200, sample_interval=10)  # 2 virtual hours

    assert sim.min_occupancy >= 0
    assert min(res.occupancies) >= 0
    assert sim.conservation_ok

    trains = _join_by_service(sim.producer.records)
    complete = [t for t in trains.values() if {"approach", "in_station", "depart"} <= t.keys()]
    assert len(complete) > 50, "expected many completed trains in a 2h run"

    for t in complete:
        p = t["approach"]["passengers"]
        st = t["in_station"]
        assert st["passengers_staying"] + st["passengers_out"] + st["passengers_station"] == p
        dep = t["depart"]
        assert dep["passengers"] == st["passengers_staying"] + dep["passengers_boarding"]
        assert dep["passengers_boarding"] >= 0
        assert dep["passengers"] <= dep["train_capacity"]
        assert t["approach"]["passengers"] <= t["approach"]["train_capacity"]


def test_emitted_records_validate_against_schemas():
    import fastavro

    parsed = {
        topic: fastavro.parse_schema(load_schema(topic))
        for topic in ("train_in_transit", "train_in_station", "passengers_flow", "station_occupancy")
    }
    cfg = EmulatorConfig()
    # validate=True makes the producer validate every record on send.
    sim = Simulator(cfg, rng=random.Random(11), producer=InMemoryEventProducer(validate=True))
    sim.run(duration_s=900, sample_interval=15)

    # double-check explicitly too
    for topic, rec in sim.producer.records:
        assert fastavro.validation.validate(rec, parsed[topic], raise_errors=True)

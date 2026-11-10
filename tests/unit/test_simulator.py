"""Simulator tests: determinism, validity of every event, realism, and chaos rates."""

from __future__ import annotations

import itertools
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest

from freightpulse.domain.enums import DlqErrorType, EventType, ShipmentStatus
from freightpulse.domain.events import EventValidationError, parse_event
from freightpulse.domain.geo import haversine_mi
from freightpulse.domain.reference_data import FACILITY_LOCATIONS
from freightpulse.domain.shipment_state import ShipmentState, apply_event, new_shipment_state
from freightpulse.simulator.chaos import CORRUPTIONS, ChaosConfig, ChaosStream, corrupt
from freightpulse.simulator.cli import build_parser, default_id_offset
from freightpulse.simulator.fleet import (
    MAX_LANE_MI,
    MIN_LANE_MI,
    Fleet,
    SimConfig,
    candidate_lanes,
    facility_dwell_median_min,
)

START = datetime(2026, 11, 10, 6, 0, tzinfo=UTC)
NOW = datetime(2026, 11, 10, 6, 0, tzinfo=UTC)


def _cfg(n: int = 20, seed: int = 7) -> SimConfig:
    return SimConfig(shipments=n, seed=seed, start=START, ramp=timedelta(minutes=60))


def test_deterministic_for_seed() -> None:
    a = [(e.event_id, e.event_time, e.event_type) for e in Fleet(_cfg()).events()]
    b = [(e.event_id, e.event_time, e.event_type) for e in Fleet(_cfg()).events()]
    c = [(e.event_id, e.event_time, e.event_type) for e in Fleet(_cfg(seed=8)).events()]
    assert a == b
    assert a != c


def test_stream_is_time_ordered_and_complete() -> None:
    events = list(Fleet(_cfg()).events())
    times = [e.event_time for e in events]
    assert times == sorted(times)
    per_type = Counter(e.event_type for e in events)
    assert per_type[EventType.SHIPMENT_CREATED] == 20
    assert per_type[EventType.SHIPMENT_DELIVERED] == 20
    assert per_type[EventType.TENDER_ACCEPTED] == 20
    assert per_type[EventType.GPS_PING] > 20 * 50


def test_every_event_is_valid_and_lifecycles_apply_cleanly() -> None:
    """Without chaos the processor's reducer accepts every simulated event."""
    states: dict[str, ShipmentState] = {}
    for ev in Fleet(_cfg(n=15)).events():
        from freightpulse.simulator.chaos import Emission

        parsed = parse_event(Emission(ev.event_time, ev).to_bytes(ev.event_time))
        if parsed.event_type is EventType.SHIPMENT_CREATED:
            states[parsed.shipment_id] = new_shipment_state(parsed, FACILITY_LOCATIONS)
            continue
        r = apply_event(states[parsed.shipment_id], parsed)
        assert r.accepted, (parsed.event_type, r.rejection)
        states[parsed.shipment_id] = r.state
    assert {s.status for s in states.values()} == {ShipmentStatus.DELIVERED}
    for s in states.values():
        dwell = s.stops[0].departed_at - s.stops[0].arrived_at  # type: ignore[operator]
        assert timedelta(minutes=10) < dwell < timedelta(hours=12)


def test_ping_cadence_and_speed_are_realistic() -> None:
    by_ship: dict[str, list[tuple[datetime, float, float, float]]] = {}
    for ev in Fleet(_cfg(n=5)).events():
        if ev.event_type is EventType.GPS_PING:
            p = ev.payload
            by_ship.setdefault(ev.shipment_id, []).append(
                (ev.event_time, p.lat, p.lon, p.speed_mph)  # type: ignore[attr-defined]
            )
    for pings in by_ship.values():
        moving = [p for p in pings if p[3] > 0]
        assert all(35 <= p[3] <= 75 for p in moving)
        for (t1, la1, lo1, s1), (t2, la2, lo2, s2) in itertools.pairwise(moving):
            gap = (t2 - t1).total_seconds()
            if gap <= 120 and s1 > 0 and s2 > 0:
                from freightpulse.domain.geo import Point

                implied = haversine_mi(Point(la1, lo1), Point(la2, lo2)) / (gap / 3600)
                assert implied < 100  # never trips the impossible-jump rule


def test_tender_rejections_near_fifteen_percent() -> None:
    counts = Counter(
        e.event_type
        for e in Fleet(_cfg(n=400, seed=3)).events()
        if e.event_type in (EventType.SHIPMENT_TENDERED, EventType.TENDER_REJECTED)
    )
    rate = counts[EventType.TENDER_REJECTED] / counts[EventType.SHIPMENT_TENDERED]
    assert 0.09 < rate < 0.2


def test_lanes_and_facility_dwell() -> None:
    lanes = candidate_lanes()
    assert lanes
    for a, b in lanes:
        assert MIN_LANE_MI <= haversine_mi(a.location, b.location) <= MAX_LANE_MI
    medians = {facility_dwell_median_min(f) for f in FACILITY_LOCATIONS}
    assert all(60 <= m <= 100 for m in medians)
    assert len(medians) > 30


def test_chaos_rates_and_ordering() -> None:
    chaos = ChaosStream(
        ChaosConfig(
            duplicate_rate=0.05,
            out_of_order_rate=0.05,
            malformed_rate=0.02,
            gps_dropout_rate=0.01,
            seed=1,
        )
    )
    emissions = list(chaos.emissions(Fleet(_cfg(n=30)).events()))
    s = chaos.stats
    generated = s["generated"] - s["dropped_gps"]
    assert 0.03 < s["duplicates"] / generated < 0.07
    assert 0.03 < s["out_of_order"] / generated < 0.07
    assert s["dropout_episodes"] > 0
    assert s["dropped_gps"] > s["dropout_episodes"]
    assert sum(v for k, v in s.items() if k.startswith("malformed:")) > 0
    # Emission order is by emit time; delayed events arrive after later events.
    emit_times = [e.emit_at for e in emissions]
    assert emit_times == sorted(emit_times)
    assert any(e.delayed for e in emissions)
    ids = Counter(e.event.event_id for e in emissions)
    assert sum(1 for c in ids.values() if c > 1) == s["duplicates"]
    # shipment.created is never corrupted.
    assert not any(
        e.corruption for e in emissions if e.event.event_type is EventType.SHIPMENT_CREATED
    )


@pytest.mark.parametrize("kind", [c for c in CORRUPTIONS if c != "teleport"])
def test_corruptions_fail_validation(kind: str) -> None:
    ev = next(e for e in Fleet(_cfg(n=1)).events() if e.event_type is EventType.GPS_PING)
    from freightpulse.simulator.chaos import Emission

    raw = corrupt(Emission(ev.event_time, ev).to_bytes(NOW), kind)
    with pytest.raises(EventValidationError):
        parse_event(raw)


def test_teleport_is_schema_valid_but_an_impossible_jump() -> None:
    from freightpulse.simulator.chaos import Emission

    events = list(Fleet(_cfg(n=1)).events())
    states: dict[str, ShipmentState] = {}
    jumped = None
    for ev in events:
        parsed = parse_event(Emission(ev.event_time, ev).to_bytes(ev.event_time))
        if parsed.event_type is EventType.SHIPMENT_CREATED:
            states[parsed.shipment_id] = new_shipment_state(parsed, FACILITY_LOCATIONS)
            continue
        state = states[parsed.shipment_id]
        if parsed.event_type is EventType.GPS_PING and state.last_ping_at is not None:
            bad = parse_event(
                corrupt(Emission(ev.event_time, ev).to_bytes(ev.event_time), "teleport")
            )
            jumped = apply_event(state, bad)
            break
        states[parsed.shipment_id] = apply_event(state, parsed).state
    assert jumped is not None
    assert jumped.rejection is not None
    assert jumped.rejection.error_type is DlqErrorType.IMPOSSIBLE_GPS_JUMP


def test_cli_defaults_match_spec() -> None:
    args = build_parser().parse_args([])
    assert args.shipments == 500
    assert args.duplicate_rate == 0.02
    assert args.out_of_order_rate == 0.03
    assert args.malformed_rate == 0.005
    assert args.gps_dropout_rate == 0.01
    assert args.acceleration == 60
    assert default_id_offset(START) != default_id_offset(START + timedelta(minutes=1))


def test_gps_dropout_rate_is_fraction_of_pings_lost() -> None:
    chaos = ChaosStream(
        ChaosConfig(
            duplicate_rate=0, out_of_order_rate=0, malformed_rate=0, gps_dropout_rate=0.05, seed=3
        )
    )
    events = list(Fleet(_cfg(n=60, seed=5)).events())
    list(chaos.emissions(iter(events)))
    pings = sum(1 for e in events if e.event_type is EventType.GPS_PING)
    lost = chaos.stats["dropped_gps"] / pings
    assert 0.02 < lost < 0.10
    # Losses are clustered into multi-ping outages (that is what trips stale tracking).
    assert chaos.stats["dropped_gps"] / chaos.stats["dropout_episodes"] > 20

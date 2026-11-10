"""State machine + reducer tests, including Hypothesis properties:

* random event sequences never produce a status regression;
* any shuffle of a valid lifecycle converges to the same final state as the ordered sequence;
* re-applying (duplicating) events never changes the result.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import timedelta

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from freightpulse.domain import state_machine as sm
from freightpulse.domain.enums import (
    AlertAction,
    AlertType,
    DlqErrorType,
    DriverStatus,
    EventType,
    LateRisk,
    Severity,
    ShipmentStatus,
)
from freightpulse.domain.events import EventEnvelope, ParsedEvent, parse_event
from freightpulse.domain.geo import Point, interpolate
from freightpulse.domain.shipment_state import (
    ApplyResult,
    ShipmentState,
    apply_event,
    compute_next_stop,
    new_shipment_state,
)
from tests.factories import DALLAS, FACILITY_LOCATIONS, MEMPHIS, T0, EventFactory

S = ShipmentStatus


@dataclass
class Folded:
    state: ShipmentState
    results: list[ApplyResult] = field(default_factory=list)
    rejected: list[ParsedEvent] = field(default_factory=list)


def fold(envs: list[EventEnvelope]) -> Folded:
    """Mimic the processor: dedupe by event_id, create on shipment.created, then reduce.

    Events seen before ``shipment.created`` are parked and replayed after it (like the processor).
    """
    parsed = [parse_event(e.to_json_bytes()) for e in envs]
    created = next(p for p in parsed if p.event_type is EventType.SHIPMENT_CREATED)
    state = new_shipment_state(created, FACILITY_LOCATIONS)
    out = Folded(state)
    seen = {created.event_id}
    for p in parsed:
        if p.event_id in seen:
            continue
        seen.add(p.event_id)
        r = apply_event(out.state, p)
        out.results.append(r)
        out.state = r.state  # like the processor: rejections may update GPS bookkeeping
        if not r.accepted:
            out.rejected.append(p)
    return out


def summary(state: ShipmentState) -> tuple[object, ...]:
    """The order-independent part of the state (EWMA/ETA depend on which pings were in order)."""
    return (
        state.status,
        state.pre_exception_status,
        state.carrier_id,
        state.rate_usd,
        tuple((s.arrived_at, s.departed_at) for s in state.stops),
        state.last_lat,
        state.last_lon,
        state.last_ping_at,
        state.last_event_time,
        state.next_stop_sequence,
    )


# --- decide() table --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("current", "target", "outcome", "result"),
    [
        (S.CREATED, S.TENDERED, sm.Outcome.APPLIED, S.TENDERED),
        (S.TENDERED, S.REJECTED, sm.Outcome.APPLIED, S.REJECTED),
        (S.REJECTED, S.TENDERED, sm.Outcome.APPLIED, S.TENDERED),
        (S.TENDERED, S.ACCEPTED, sm.Outcome.APPLIED, S.ACCEPTED),
        (S.ACCEPTED, S.IN_TRANSIT, sm.Outcome.APPLIED, S.IN_TRANSIT),  # missed geofence
        (S.IN_TRANSIT, S.IN_TRANSIT, sm.Outcome.NOOP, S.IN_TRANSIT),
        (S.DELIVERED, S.IN_TRANSIT, sm.Outcome.INVALID, S.DELIVERED),
        (S.DELIVERED, S.DELIVERED, sm.Outcome.NOOP, S.DELIVERED),
        (S.AT_DELIVERY, S.AT_PICKUP, sm.Outcome.INVALID, S.AT_DELIVERY),
        (S.ACCEPTED, S.TENDERED, sm.Outcome.INVALID, S.ACCEPTED),
    ],
)
def test_decide_table(
    current: ShipmentStatus, target: ShipmentStatus, outcome: sm.Outcome, result: ShipmentStatus
) -> None:
    d = sm.decide(sm.StatusView(current), target)
    assert d.outcome is outcome
    assert d.view.status is result


def test_exception_overlay_and_resolution() -> None:
    v = sm.StatusView(S.IN_TRANSIT)
    d = sm.decide(v, sm.Target.RAISE_EXCEPTION)
    assert d.view == sm.StatusView(S.EXCEPTION, S.IN_TRANSIT)
    assert sm.decide(d.view, sm.Target.RAISE_EXCEPTION).outcome is sm.Outcome.NOOP
    assert sm.decide(d.view, sm.Target.RESOLVE_EXCEPTION).view == sm.StatusView(S.IN_TRANSIT)
    assert sm.decide(d.view, S.IN_TRANSIT).view == sm.StatusView(S.IN_TRANSIT)
    assert sm.decide(d.view, S.AT_DELIVERY).view == sm.StatusView(S.AT_DELIVERY)
    assert sm.decide(d.view, S.AT_PICKUP).outcome is sm.Outcome.INVALID
    assert sm.decide(v, sm.Target.RESOLVE_EXCEPTION).outcome is sm.Outcome.NOOP
    assert sm.decide(sm.StatusView(S.DELIVERED), sm.Target.RAISE_EXCEPTION).outcome is (
        sm.Outcome.INVALID
    )
    # An older event can advance the state *under* the overlay, never the overlay itself.
    assert sm.absorb_out_of_order(d.view, S.AT_DELIVERY) == sm.StatusView(
        S.EXCEPTION, S.AT_DELIVERY
    )
    assert sm.absorb_out_of_order(d.view, S.ACCEPTED) == d.view
    assert sm.absorb_out_of_order(v, S.DELIVERED) == v


def test_stop_event_targets() -> None:
    a, d = EventType.STOP_ARRIVED, EventType.STOP_DEPARTED
    assert sm.stop_event_target(a, 1, 3) is S.AT_PICKUP
    assert sm.stop_event_target(a, 2, 3) is None
    assert sm.stop_event_target(a, 3, 3) is S.AT_DELIVERY
    assert sm.stop_event_target(d, 1, 3) is S.IN_TRANSIT
    assert sm.stop_event_target(d, 2, 3) is S.IN_TRANSIT
    assert sm.stop_event_target(d, 3, 3) is None
    with pytest.raises(ValueError, match="not a stop event"):
        sm.stop_event_target(EventType.GPS_PING, 1, 2)


# --- reducer ---------------------------------------------------------------------------------


def test_full_lifecycle_in_order() -> None:
    f = EventFactory()
    out = fold(f.full_lifecycle(pings=6))
    assert not out.rejected
    s = out.state
    assert s.status is S.DELIVERED
    assert s.carrier_id == "CAR-001"
    assert str(s.rate_usd) == "2450.00"
    assert all(st.arrived_at and st.departed_at for st in s.stops)
    assert s.next_stop_sequence is None and s.eta is None
    assert s.speed.mean == pytest.approx(60.0)
    assert not any(r.out_of_order for r in out.results)


def test_eta_and_late_risk_alerts_emit_on_level_change_only() -> None:
    f = EventFactory()
    dal, mem = DALLAS[1], MEMPHIS[1]
    # A tight delivery window: ~8.3h of driving remaining but the window closes in 5h.
    envs = [
        f.created(T0, delivery_window=(T0 + timedelta(hours=4), T0 + timedelta(hours=5))),
        f.tendered(T0 + timedelta(minutes=1)),
        f.accepted(T0 + timedelta(minutes=2)),
        f.arrived(T0 + timedelta(minutes=10), 1, DALLAS[0]),
        f.departed(T0 + timedelta(minutes=20), 1, DALLAS[0]),
    ]
    for i in range(5):
        envs.append(
            f.ping(T0 + timedelta(minutes=30 + i * 5), interpolate(dal, mem, 0.01 * (i + 1)))
        )
    out = fold(envs)
    s = out.state
    assert s.status is S.IN_TRANSIT
    assert s.late_risk is LateRisk.HIGH
    assert s.eta is not None and s.eta_confidence is not None and s.miles_remaining is not None
    assert s.next_stop_sequence == 2
    alerts = [a for r in out.results for a in r.alerts]
    assert [a.severity for a in alerts] == [Severity.HIGH]  # emitted once, not per ping

    # Arriving at the delivery stop clears the late risk.
    r = apply_event(
        s, parse_event(f.arrived(T0 + timedelta(hours=6), 2, MEMPHIS[0]).to_json_bytes())
    )
    assert r.state.status is S.AT_DELIVERY
    assert r.state.late_risk is LateRisk.LOW
    assert [a.action for a in r.alerts] == [AlertAction.RESOLVE]


def test_late_risk_medium_then_low_on_recovery() -> None:
    f = EventFactory()
    dal, mem = DALLAS[1], MEMPHIS[1]
    created = parse_event(f.created(T0).to_json_bytes())
    state = new_shipment_state(created, FACILITY_LOCATIONS)
    # Put the truck 10 road-miles (at 60mph) out with the window closing in 20-40 minutes.
    near = interpolate(dal, mem, 0.98)
    stops = list(state.stops)
    from dataclasses import replace

    stops[0] = replace(stops[0], arrived_at=T0, departed_at=T0)
    state = replace(state, stops=tuple(stops), status=S.IN_TRANSIT, next_stop_sequence=2)
    end = T0 + timedelta(hours=1)
    stops[1] = replace(stops[1], appt_end=end)
    state = replace(state, stops=tuple(stops))
    r1 = apply_event(state, parse_event(f.ping(T0 + timedelta(minutes=40), near).to_json_bytes()))
    assert r1.state.late_risk is LateRisk.MEDIUM
    assert r1.alerts[0].alert_type is AlertType.LATE_RISK
    r2 = apply_event(
        r1.state, parse_event(f.ping(T0 + timedelta(minutes=41), near).to_json_bytes())
    )
    assert r2.alerts == ()
    r3 = apply_event(
        r2.state, parse_event(f.ping(T0 + timedelta(minutes=10), near).to_json_bytes())
    )  # older ping: ignored for position
    assert r3.out_of_order and r3.state.last_ping_at == r2.state.last_ping_at


def test_invalid_transition_rejected() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=1)
    late_departure = f.departed(T0 + timedelta(days=2), 1, DALLAS[0])
    out = fold([*envs, late_departure])
    assert out.state.status is S.DELIVERED
    assert [r.rejection.error_type for r in out.results if r.rejection] == [
        DlqErrorType.INVALID_TRANSITION
    ]


def test_out_of_order_older_event_backfills_without_regressing() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=2)
    arrived_pickup = envs[3]
    reordered = [e for e in envs if e is not arrived_pickup] + [arrived_pickup]
    out = fold(reordered)
    assert out.state.status is S.DELIVERED
    assert out.state.stops[0].arrived_at == arrived_pickup.event_time
    last = out.results[-1]
    assert last.out_of_order and last.lateness_seconds > 0 and last.stops_changed


def test_impossible_gps_jump_rejected() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=2)[:6]
    ping = envs[-1]
    jump = f.ping(ping.event_time + timedelta(minutes=5), Point(47.6, -122.3))  # Seattle
    out = fold([*envs, jump])
    rej = [r.rejection for r in out.results if r.rejection]
    assert rej and rej[0].error_type is DlqErrorType.IMPOSSIBLE_GPS_JUMP
    assert out.state.last_lat == ping.payload["lat"]


def test_stop_payload_mismatch_rejected() -> None:
    f = EventFactory()
    out = fold(
        [
            f.created(T0),
            f.arrived(T0 + timedelta(hours=1), 5, DALLAS[0]),
            f.arrived(T0 + timedelta(hours=1), 1, MEMPHIS[0]),
        ]
    )
    assert [r.rejection.error_type for r in out.results if r.rejection] == [
        DlqErrorType.SCHEMA_VALIDATION,
        DlqErrorType.SCHEMA_VALIDATION,
    ]


def test_re_tender_after_rejection_keeps_accepting_carrier() -> None:
    f = EventFactory()
    envs = [
        f.created(T0),
        f.tendered(T0 + timedelta(minutes=1), carrier="CAR-001", rate="2000.00"),
        f.rejected(T0 + timedelta(minutes=2), carrier="CAR-001"),
        f.tendered(T0 + timedelta(minutes=3), carrier="CAR-002", rate="2300.00"),
        f.accepted(T0 + timedelta(minutes=4), carrier="CAR-002"),
    ]
    for order in (envs, [envs[0], *reversed(envs[1:])]):
        s = fold(order).state
        assert s.status is S.ACCEPTED
        assert s.carrier_id == "CAR-002"
        assert str(s.rate_usd) == "2300.00"


def test_duplicate_created_and_exception_flow() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=4, with_exception=True)
    out = fold([*envs, f.created(T0 + timedelta(minutes=1))])
    assert out.state.status is S.DELIVERED
    statuses = [r.state.status for r in out.results]
    assert S.EXCEPTION in statuses


def test_compute_next_stop() -> None:
    f = EventFactory()
    s = new_shipment_state(parse_event(f.created().to_json_bytes()), FACILITY_LOCATIONS)
    assert compute_next_stop(s.stops) == 1
    from dataclasses import replace

    stops = (replace(s.stops[0], departed_at=T0), s.stops[1])
    assert compute_next_stop(stops) == 2
    stops = (s.stops[0], replace(s.stops[1], arrived_at=T0))
    assert compute_next_stop(stops) is None


# --- properties ------------------------------------------------------------------------------

LIFECYCLE_TYPES = [
    EventType.SHIPMENT_TENDERED,
    EventType.TENDER_ACCEPTED,
    EventType.TENDER_REJECTED,
    EventType.STOP_ARRIVED,
    EventType.STOP_DEPARTED,
    EventType.SHIPMENT_DELIVERED,
    EventType.SHIPMENT_EXCEPTION,
    EventType.GPS_PING,
]


def _random_event(
    f: EventFactory, kind: EventType, minute: int, seq: int, flag: bool
) -> EventEnvelope:
    at = T0 + timedelta(minutes=minute)
    facility = DALLAS[0] if seq == 1 else MEMPHIS[0]
    match kind:
        case EventType.SHIPMENT_TENDERED:
            return f.tendered(at)
        case EventType.TENDER_ACCEPTED:
            return f.accepted(at)
        case EventType.TENDER_REJECTED:
            return f.rejected(at)
        case EventType.STOP_ARRIVED:
            return f.arrived(at, seq, facility)
        case EventType.STOP_DEPARTED:
            return f.departed(at, seq, facility)
        case EventType.SHIPMENT_DELIVERED:
            return f.delivered(at)
        case EventType.SHIPMENT_EXCEPTION:
            return f.exception(at, resolved=flag)
        case _:
            return f.ping(at, interpolate(DALLAS[1], MEMPHIS[1], (minute % 600) / 600), speed=55)


random_events = st.lists(
    st.tuples(
        st.sampled_from(LIFECYCLE_TYPES),
        st.integers(min_value=1, max_value=600),
        st.integers(min_value=1, max_value=2),
        st.booleans(),
    ),
    max_size=40,
)


@settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
@given(random_events)
def test_random_sequences_never_regress_status(
    spec: list[tuple[EventType, int, int, bool]],
) -> None:
    f = EventFactory()
    state = new_shipment_state(parse_event(f.created(T0).to_json_bytes()), FACILITY_LOCATIONS)
    for kind, minute, seq, flag in spec:
        r = apply_event(
            state, parse_event(_random_event(f, kind, minute, seq, flag).to_json_bytes())
        )
        if not r.accepted:
            assert r.rejection is not None
            # Rejections never touch business state; at most they remember a suspect GPS fix.
            from dataclasses import replace as _replace

            assert _replace(r.state, suspect_lat=None, suspect_lon=None, suspect_at=None) == (
                _replace(state, suspect_lat=None, suspect_lon=None, suspect_at=None)
            )
            continue
        assert not sm.is_regression(state.view, r.state.view), (state.view, r.state.view, kind)
        assert r.state.last_event_time >= state.last_event_time
        if state.last_ping_at is not None:
            assert r.state.last_ping_at is not None and r.state.last_ping_at >= state.last_ping_at
        state = r.state
    assert state.status in set(ShipmentStatus)


@settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
@given(st.randoms(use_true_random=False), st.booleans(), st.integers(min_value=0, max_value=8))
def test_shuffled_lifecycle_converges_to_ordered_state(
    rnd: random.Random, with_exception: bool, pings: int
) -> None:
    f = EventFactory()
    ordered = f.full_lifecycle(pings=pings, with_exception=with_exception)
    created, rest = ordered[0], ordered[1:]
    shuffled = rest[:]
    rnd.shuffle(shuffled)
    a = fold(ordered)
    b = fold([created, *shuffled])
    assert not a.rejected and not b.rejected
    assert summary(a.state) == summary(b.state)


@settings(max_examples=100)
@given(st.randoms(use_true_random=False))
def test_duplicates_are_idempotent(rnd: random.Random) -> None:
    f = EventFactory()
    ordered = f.full_lifecycle(pings=5)
    with_dups = [*ordered]
    for e in rnd.sample(ordered[1:], k=4):
        # A redelivered duplicate always arrives after the original.
        first = next(i for i, x in enumerate(with_dups) if x is e)
        with_dups.insert(rnd.randrange(first + 1, len(with_dups) + 1), e)
    # Dedupe by event_id (as the processor does) gives the identical state...
    assert fold(with_dups).state == fold(ordered).state
    # ...and even re-applying an already-applied lifecycle event is a no-op for status.
    s = fold(ordered).state
    for e in ordered[1:]:
        if e.event_type is not EventType.GPS_PING:
            r = apply_event(s, parse_event(e.to_json_bytes()))
            assert r.state.status is s.status


def test_ping_status_drives_hos_in_state() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=1)[:5]
    t = envs[-1].event_time
    envs += [
        f.ping(t + timedelta(minutes=10), DALLAS[1], speed=0, status=DriverStatus.SLEEPER),
        f.ping(t + timedelta(hours=11), DALLAS[1], speed=0, status=DriverStatus.SLEEPER),
    ]
    s = fold(envs).state
    assert s.hos.drive_hours_used == 0.0
    assert s.hos.rest_started_at is not None


def test_bad_first_fix_is_replaced_by_reanchoring() -> None:
    """A corrupt *first* ping cannot be detected; the next two good pings re-anchor the track."""
    f = EventFactory()
    envs = f.full_lifecycle(pings=1)[:5]  # up to departed pickup
    t = envs[-1].event_time
    good = [interpolate(DALLAS[1], MEMPHIS[1], 0.001 * i) for i in (1, 2, 3)]
    envs += [
        f.ping(t + timedelta(minutes=1), Point(70.0, -40.0)),  # teleport, no anchor yet
        f.ping(t + timedelta(minutes=2), good[0]),  # rejected vs bad anchor, becomes suspect
        f.ping(t + timedelta(minutes=3), good[1]),  # agrees with suspect: re-anchor
        f.ping(t + timedelta(minutes=4), good[2]),
    ]
    out = fold(envs)
    assert [r.rejection.error_type for r in out.results if r.rejection] == [
        DlqErrorType.IMPOSSIBLE_GPS_JUMP
    ]
    assert (out.state.last_lat, out.state.last_lon) == (good[2].lat, good[2].lon)
    assert out.state.suspect_at is None


def test_isolated_teleport_does_not_reanchor() -> None:
    f = EventFactory()
    envs = f.full_lifecycle(pings=1)[:5]
    t = envs[-1].event_time
    a, b = interpolate(DALLAS[1], MEMPHIS[1], 0.001), interpolate(DALLAS[1], MEMPHIS[1], 0.002)
    envs += [
        f.ping(t + timedelta(minutes=1), a),
        f.ping(t + timedelta(minutes=2), Point(70.0, -40.0)),
        f.ping(t + timedelta(minutes=3), b),  # far from the suspect, close to the anchor
    ]
    out = fold(envs)
    assert len(out.rejected) == 1
    assert (out.state.last_lat, out.state.last_lon) == (b.lat, b.lon)

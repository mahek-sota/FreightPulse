"""The pure per-shipment reducer: ``apply_event(state, event) -> ApplyResult``.

The stream processor loads a :class:`ShipmentState` from Postgres, folds each newly seen
event into it with :func:`apply_event`, then persists the result. All business rules live here
so they can be unit- and property-tested without Kafka or a database.

Event-time guards (docs/adr/0003-out-of-order-handling.md):

* lifecycle status changes are guarded by ``status_event_time`` (the time of the lifecycle event
  that set the current status);
* position/ETA fields are guarded by ``last_ping_at``;
* ``last_event_time`` is the max event time seen for the shipment (used for the lateness metric).

Using one guard for everything would discard valid GPS pings that happen to be processed after a
slightly newer lifecycle event from the *other* topic, which is routine because the two topics are
consumed independently.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final
from uuid import UUID

from freightpulse.domain import state_machine as sm
from freightpulse.domain.alerts import (
    AlertMessage,
    late_risk_alert,
    late_risk_cleared,
)
from freightpulse.domain.enums import (
    DlqErrorType,
    EventType,
    LateRisk,
    ShipmentStatus,
    StopType,
)
from freightpulse.domain.eta import (
    HosClock,
    SpeedStats,
    classify_late_risk,
    estimate_eta,
    update_hos,
    update_speed,
)
from freightpulse.domain.events import (
    ExceptionPayload,
    GpsPingPayload,
    ParsedEvent,
    ShipmentCreatedPayload,
    StopEventPayload,
    TenderedPayload,
    TenderResponsePayload,
)
from freightpulse.domain.geo import Point, haversine_mi

MAX_PLAUSIBLE_SPEED_MPH: Final = 100.0
MIN_JUMP_INTERVAL: Final = timedelta(seconds=1)
REANCHOR_WINDOW: Final = timedelta(minutes=30)
ETA_STATUSES: Final = frozenset(
    {
        ShipmentStatus.ACCEPTED,
        ShipmentStatus.AT_PICKUP,
        ShipmentStatus.IN_TRANSIT,
        ShipmentStatus.AT_DELIVERY,
    }
)


@dataclass(frozen=True, slots=True)
class StopState:
    stop_sequence: int
    facility_id: str
    stop_type: StopType
    location: Point
    appt_start: datetime
    appt_end: datetime
    arrived_at: datetime | None = None
    departed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ShipmentState:
    shipment_id: str
    status: ShipmentStatus
    status_event_time: datetime
    stops: tuple[StopState, ...]
    last_event_time: datetime
    pre_exception_status: ShipmentStatus | None = None
    carrier_id: str | None = None
    carrier_event_time: datetime | None = None
    rate_usd: Decimal | None = None
    rate_event_time: datetime | None = None
    last_lat: float | None = None
    last_lon: float | None = None
    last_ping_at: datetime | None = None
    speed: SpeedStats = field(default_factory=SpeedStats)
    hos: HosClock = field(default_factory=HosClock)
    next_stop_sequence: int | None = 1
    eta: datetime | None = None
    eta_confidence: float | None = None
    miles_remaining: float | None = None
    late_risk: LateRisk = LateRisk.LOW
    late_risk_stop_sequence: int | None = None
    # Last ping rejected as an impossible jump. If the next ping agrees with it rather than
    # with the anchor, the anchor was the outlier and the track re-anchors (see _apply_ping).
    suspect_lat: float | None = None
    suspect_lon: float | None = None
    suspect_at: datetime | None = None
    version: int = 0

    @property
    def view(self) -> sm.StatusView:
        return sm.StatusView(self.status, self.pre_exception_status)

    @property
    def position(self) -> Point | None:
        if self.last_lat is None or self.last_lon is None:
            return None
        return Point(self.last_lat, self.last_lon)

    def stop(self, sequence: int) -> StopState | None:
        if 1 <= sequence <= len(self.stops):
            return self.stops[sequence - 1]
        return None


@dataclass(frozen=True, slots=True)
class Rejection:
    error_type: DlqErrorType
    detail: str


@dataclass(frozen=True, slots=True)
class ApplyResult:
    state: ShipmentState
    rejection: Rejection | None = None
    out_of_order: bool = False
    lateness_seconds: float = 0.0
    alerts: tuple[AlertMessage, ...] = ()
    stops_changed: bool = False
    state_changed: bool = False

    @property
    def accepted(self) -> bool:
        """Accepted events are appended to the event log; rejected ones go to the DLQ."""
        return self.rejection is None


# --- construction --------------------------------------------------------------------------


def new_shipment_state(event: ParsedEvent, facility_locations: dict[str, Point]) -> ShipmentState:
    """Initial state from a ``shipment.created`` event.

    Raises ``KeyError`` if a stop references an unknown facility (caller routes to the DLQ).
    """
    payload = event.payload
    if not isinstance(payload, ShipmentCreatedPayload):  # pragma: no cover - caller guarantees
        raise TypeError("expected shipment.created")
    stops = tuple(
        StopState(
            stop_sequence=s.stop_sequence,
            facility_id=s.facility_id,
            stop_type=s.stop_type,
            location=facility_locations[s.facility_id],
            appt_start=s.appt_start,
            appt_end=s.appt_end,
        )
        for s in payload.stops
    )
    return ShipmentState(
        shipment_id=event.shipment_id,
        status=ShipmentStatus.CREATED,
        status_event_time=event.event_time,
        stops=stops,
        last_event_time=event.event_time,
        next_stop_sequence=1,
        late_risk_stop_sequence=1,
    )


# --- helpers -------------------------------------------------------------------------------


def compute_next_stop(stops: tuple[StopState, ...]) -> int | None:
    """The stop after the furthest stop with any arrival/departure evidence."""
    furthest = 0
    for s in stops:
        if s.arrived_at is not None or s.departed_at is not None:
            furthest = max(furthest, s.stop_sequence)
    nxt = furthest + 1
    return nxt if nxt <= len(stops) else None


def _with_stop(
    stops: tuple[StopState, ...], seq: int, *, arrived: datetime | None, departed: datetime | None
) -> tuple[tuple[StopState, ...], bool]:
    """Backfill stop timestamps; earliest arrival and latest departure win (order-independent)."""
    out = list(stops)
    s = out[seq - 1]
    new_arr = s.arrived_at
    new_dep = s.departed_at
    if arrived is not None:
        new_arr = arrived if new_arr is None else min(new_arr, arrived)
    if departed is not None:
        new_dep = departed if new_dep is None else max(new_dep, departed)
    if new_arr == s.arrived_at and new_dep == s.departed_at:
        return stops, False
    out[seq - 1] = replace(s, arrived_at=new_arr, departed_at=new_dep)
    return tuple(out), True


def _lifecycle_target(
    event: ParsedEvent, state: ShipmentState
) -> ShipmentStatus | sm.Target | None:
    et = event.event_type
    if et in sm.SIMPLE_TARGETS:
        return sm.SIMPLE_TARGETS[et]
    if et in (EventType.STOP_ARRIVED, EventType.STOP_DEPARTED):
        assert isinstance(event.payload, StopEventPayload)
        return sm.stop_event_target(et, event.payload.stop_sequence, len(state.stops))
    if et is EventType.SHIPMENT_EXCEPTION:
        assert isinstance(event.payload, ExceptionPayload)
        return sm.Target.RESOLVE_EXCEPTION if event.payload.resolved else sm.Target.RAISE_EXCEPTION
    return None  # pragma: no cover - gps handled separately


def _refresh_eta(
    state: ShipmentState, *, now: datetime, trigger_event_id: UUID
) -> tuple[ShipmentState, list[AlertMessage]]:
    """Recompute ETA/late risk for the next stop and emit an alert on a level change."""
    alerts: list[AlertMessage] = []
    nxt = compute_next_stop(state.stops)

    # Moving on to a different stop (or none) clears the previous stop's late risk.
    if state.late_risk_stop_sequence != nxt and state.late_risk is not LateRisk.LOW:
        alerts.append(
            late_risk_cleared(
                shipment_id=state.shipment_id,
                stop_sequence=state.late_risk_stop_sequence or 0,
                reason="stop reached" if nxt is not None else "delivered",
                at=now,
            )
        )
        state = replace(state, late_risk=LateRisk.LOW)
    state = replace(state, next_stop_sequence=nxt, late_risk_stop_sequence=nxt)

    position = state.position
    stop = state.stop(nxt) if nxt is not None else None
    if (
        stop is None
        or position is None
        or state.last_ping_at is None
        or state.view.base not in ETA_STATUSES
    ):
        if stop is None:
            state = replace(state, eta=None, eta_confidence=None, miles_remaining=None)
        return state, alerts

    gap_min = max(0.0, (now - state.last_ping_at).total_seconds() / 60)
    est = estimate_eta(
        now=now,
        position=position,
        destination=stop.location,
        speed=state.speed,
        hos=state.hos,
        ping_gap_minutes=gap_min,
    )
    level = classify_late_risk(est.eta, stop.appt_end)
    alert = late_risk_alert(
        shipment_id=state.shipment_id,
        stop_sequence=stop.stop_sequence,
        previous=state.late_risk,
        level=level,
        eta=est.eta,
        appt_end=stop.appt_end,
        trigger_event_id=trigger_event_id,
        at=now,
    )
    if alert is not None:
        alerts.append(alert)
    state = replace(
        state,
        eta=est.eta,
        eta_confidence=est.confidence,
        miles_remaining=est.miles_remaining,
        late_risk=level,
    )
    return state, alerts


# --- the reducer ---------------------------------------------------------------------------


def apply_event(state: ShipmentState, event: ParsedEvent) -> ApplyResult:
    """Fold one *newly seen* event (already de-duplicated by event_id) into the state."""
    if event.event_type is EventType.GPS_PING:
        return _apply_ping(state, event)
    if event.event_type is EventType.SHIPMENT_CREATED:
        # A second, different shipment.created for an existing shipment: keep the first.
        return ApplyResult(state=state)
    return _apply_lifecycle(state, event)


def _lateness(state: ShipmentState, t: datetime) -> tuple[bool, float]:
    if t < state.last_event_time:
        return True, (state.last_event_time - t).total_seconds()
    return False, 0.0


def _implied_speed(a: Point, ta: datetime, b: Point, tb: datetime) -> tuple[float, float]:
    miles = haversine_mi(a, b)
    hours = max(abs(tb - ta), MIN_JUMP_INTERVAL).total_seconds() / 3600
    return miles / hours, miles


def _agrees_with_suspect(state: ShipmentState, here: Point, t: datetime) -> bool:
    """True if this ping is consistent with the previously rejected one (re-anchor)."""
    if state.suspect_lat is None or state.suspect_lon is None or state.suspect_at is None:
        return False
    if not state.suspect_at < t <= state.suspect_at + REANCHOR_WINDOW:
        return False
    implied, _ = _implied_speed(
        Point(state.suspect_lat, state.suspect_lon), state.suspect_at, here, t
    )
    return implied <= MAX_PLAUSIBLE_SPEED_MPH


def _apply_ping(state: ShipmentState, event: ParsedEvent) -> ApplyResult:
    p = event.payload
    assert isinstance(p, GpsPingPayload)
    t = event.event_time
    out_of_order, lateness = _lateness(state, t)
    last_event_time = max(state.last_event_time, t)

    if state.last_ping_at is not None and t <= state.last_ping_at:
        # Older than the position we already have: append to the log only.
        return ApplyResult(
            state=replace(state, last_event_time=last_event_time),
            out_of_order=out_of_order,
            lateness_seconds=lateness,
        )

    here = Point(p.lat, p.lon)
    prev = state.position
    if prev is not None and state.last_ping_at is not None:
        implied, miles = _implied_speed(prev, state.last_ping_at, here, t)
        if (
            implied > MAX_PLAUSIBLE_SPEED_MPH
            and miles > 0.5
            and not _agrees_with_suspect(state, here, t)
        ):
            # Remember it: if the *next* ping agrees with this one instead of the anchor, the
            # anchor itself was the bad fix (e.g. a corrupt first ping) and we re-anchor.
            return ApplyResult(
                state=replace(state, suspect_lat=p.lat, suspect_lon=p.lon, suspect_at=t),
                rejection=Rejection(
                    DlqErrorType.IMPOSSIBLE_GPS_JUMP,
                    f"implied speed {implied:.0f} mph over {miles:.1f} mi since "
                    f"{state.last_ping_at.isoformat()}",
                ),
                state_changed=True,
            )

    new = replace(
        state,
        last_lat=p.lat,
        last_lon=p.lon,
        last_ping_at=t,
        last_event_time=last_event_time,
        speed=update_speed(state.speed, p.speed_mph),
        hos=update_hos(state.hos, p.driver_status, t, state.last_ping_at),
        suspect_lat=None,
        suspect_lon=None,
        suspect_at=None,
    )
    new, alerts = _refresh_eta(new, now=t, trigger_event_id=event.event_id)
    return ApplyResult(
        state=new,
        out_of_order=out_of_order,
        lateness_seconds=lateness,
        alerts=tuple(alerts),
        state_changed=True,
    )


def _apply_lifecycle(state: ShipmentState, event: ParsedEvent) -> ApplyResult:
    t = event.event_time
    out_of_order, lateness = _lateness(state, t)
    new = replace(state, last_event_time=max(state.last_event_time, t))
    stops_changed = False
    status_applied = False

    # 1. Payload side effects that are order-independent (latest event time wins).
    payload = event.payload
    if isinstance(payload, TenderedPayload):
        if new.rate_event_time is None or t >= new.rate_event_time:
            new = replace(new, rate_usd=payload.rate_usd, rate_event_time=t)
    elif isinstance(payload, TenderResponsePayload):
        accepted = event.event_type is EventType.TENDER_ACCEPTED
        if accepted and (new.carrier_event_time is None or t >= new.carrier_event_time):
            new = replace(new, carrier_id=payload.carrier_id, carrier_event_time=t)
    elif isinstance(payload, StopEventPayload):
        stop = new.stop(payload.stop_sequence)
        if stop is None:
            return ApplyResult(
                state=state,
                rejection=Rejection(
                    DlqErrorType.SCHEMA_VALIDATION,
                    f"stop_sequence {payload.stop_sequence} not on shipment "
                    f"(has {len(state.stops)} stops)",
                ),
            )
        if stop.facility_id != payload.facility_id:
            return ApplyResult(
                state=state,
                rejection=Rejection(
                    DlqErrorType.SCHEMA_VALIDATION,
                    f"facility {payload.facility_id} does not match stop "
                    f"{payload.stop_sequence} ({stop.facility_id})",
                ),
            )

    # 2. Status transition, guarded by the event time of the current status.
    target = _lifecycle_target(event, new)
    if target is not None:
        if t >= new.status_event_time:
            decision = sm.decide(new.view, target)
            if decision.outcome is sm.Outcome.INVALID:
                return ApplyResult(
                    state=state,
                    rejection=Rejection(DlqErrorType.INVALID_TRANSITION, decision.reason),
                )
            if decision.outcome is sm.Outcome.APPLIED:
                status_applied = True
                new = replace(
                    new,
                    status=decision.view.status,
                    pre_exception_status=decision.view.pre_exception_status,
                    status_event_time=t,
                )
        else:
            absorbed = sm.absorb_out_of_order(new.view, target)
            new = replace(new, pre_exception_status=absorbed.pre_exception_status)

    # 3. Backfill stop timestamps whatever the arrival order.
    if isinstance(payload, StopEventPayload):
        is_arrival = event.event_type is EventType.STOP_ARRIVED
        stops, stops_changed = _with_stop(
            new.stops,
            payload.stop_sequence,
            arrived=t if is_arrival else None,
            departed=None if is_arrival else t,
        )
        new = replace(new, stops=stops)

    # 4. ETA / late risk follow the stop progression and terminal status.
    alerts: list[AlertMessage] = []
    if new.status is ShipmentStatus.DELIVERED and state.status is not ShipmentStatus.DELIVERED:
        if new.late_risk is not LateRisk.LOW:
            alerts.append(
                late_risk_cleared(
                    shipment_id=new.shipment_id,
                    stop_sequence=new.late_risk_stop_sequence or 0,
                    reason="delivered",
                    at=t,
                )
            )
        new = replace(
            new,
            eta=None,
            eta_confidence=None,
            miles_remaining=0.0,
            late_risk=LateRisk.LOW,
            next_stop_sequence=None,
            late_risk_stop_sequence=None,
        )
    elif stops_changed or status_applied:
        # Evaluate at the newest instant we know the position for: lifecycle events from the
        # events topic are routinely processed after newer pings from the telemetry topic.
        now = max(t, new.last_ping_at) if new.last_ping_at is not None else t
        new, alerts = _refresh_eta(new, now=now, trigger_event_id=event.event_id)

    return ApplyResult(
        state=new,
        out_of_order=out_of_order,
        lateness_seconds=lateness,
        alerts=tuple(alerts),
        stops_changed=stops_changed,
        state_changed=new != state,
    )

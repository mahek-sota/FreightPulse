"""Deterministic discrete-event fleet simulation (simulated time, no I/O).

Each shipment is a generator that yields its events in event-time order: created, tendered
(with ~15% rejections and re-tenders), accepted, deadhead to pickup, dwell (log-normal),
great-circle line-haul at 50-65 mph with noise and HOS breaks (11h driving -> 10h rest),
occasional exceptions, delivery dwell, departure and POD. Pings are emitted every 30-120 s of
simulated time.

Generators are merged with a heap into one time-ordered stream. Every shipment has its own RNG
derived from the seed, so the generated events are identical for a given ``--seed`` and
``--start`` regardless of how the streams interleave.
"""

from __future__ import annotations

import hashlib
import heapq
import math
import random
import uuid
from collections.abc import Generator, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from pydantic import BaseModel

from freightpulse.domain.enums import (
    DriverStatus,
    Equipment,
    EventType,
    ExceptionCode,
    Mode,
    StopType,
)
from freightpulse.domain.eta import HOS_MAX_DRIVING_HOURS, HOS_REQUIRED_BREAK_HOURS, required_breaks
from freightpulse.domain.events import (
    DeliveredPayload,
    ExceptionPayload,
    GpsPingPayload,
    ShipmentCreatedPayload,
    StopEventPayload,
    StopSpec,
    TenderedPayload,
    TenderResponsePayload,
)
from freightpulse.domain.geo import (
    ROAD_CIRCUITY_FACTOR,
    Point,
    haversine_mi,
    initial_bearing_deg,
    interpolate,
    offset,
    road_miles,
)
from freightpulse.domain.reference_data import CARRIERS, CUSTOMERS, FACILITIES, Facility

MIN_LANE_MI: Final = 120.0
MAX_LANE_MI: Final = 1100.0
PLANNING_SPEED_MPH: Final = 57.5

SOURCE_TMS: Final = "tms"
SOURCE_CARRIER: Final = "carrier-api"
SOURCE_ELD: Final = "eld:samsara-sim"


@dataclass(frozen=True, slots=True)
class SimConfig:
    shipments: int = 500
    seed: int = 42
    start: datetime = field(default_factory=lambda: datetime(2026, 1, 1))
    ramp: timedelta = timedelta(hours=2)
    """Shipment creation times are spread uniformly over this simulated window."""
    tender_reject_rate: float = 0.15
    exception_rate: float = 0.03
    id_offset: int = 0
    min_ping_s: float = 30.0
    max_ping_s: float = 120.0


@dataclass(frozen=True, slots=True)
class SimEvent:
    """One event in simulated time, not yet serialised (produced_at is set at send time)."""

    event_time: datetime
    event_id: uuid.UUID
    event_type: EventType
    shipment_id: str
    source: str
    payload: BaseModel

    @property
    def is_telemetry(self) -> bool:
        return self.event_type is EventType.GPS_PING


def _uuid(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def _stable_unit(text: str) -> float:
    """Deterministic value in [0, 1) from a string (per-facility characteristics)."""
    return int(hashlib.sha256(text.encode()).hexdigest()[:8], 16) / 0x1_0000_0000


def facility_dwell_median_min(facility_id: str) -> float:
    """Each facility has a characteristic median dwell between 60 and 100 minutes."""
    return 60.0 + 40.0 * _stable_unit(facility_id)


def _round_to(t: datetime, minutes: int) -> datetime:
    epoch = t.timestamp()
    step = minutes * 60
    return datetime.fromtimestamp(round(epoch / step) * step, tz=t.tzinfo)


def candidate_lanes(
    facilities: tuple[Facility, ...] = FACILITIES,
) -> list[tuple[Facility, Facility]]:
    lanes = []
    for a in facilities:
        for b in facilities:
            if a.facility_id != b.facility_id:
                d = haversine_mi(a.location, b.location)
                if MIN_LANE_MI <= d <= MAX_LANE_MI:
                    lanes.append((a, b))
    return lanes


class ShipmentScript:
    """Generates one shipment's lifecycle in event-time order."""

    def __init__(self, cfg: SimConfig, index: int, lanes: list[tuple[Facility, Facility]]) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed * 1_000_003 + index)
        self.shipment_id = f"SHP-{cfg.id_offset + index + 1:06d}"
        self.origin, self.dest = self.rng.choice(lanes)
        self.created_at = cfg.start + timedelta(
            seconds=self.rng.uniform(0, cfg.ramp.total_seconds())
        )
        self.hos_used = self.rng.uniform(0.0, 5.0)  # driver arrives with some hours used
        self.odometer = self.rng.uniform(50_000, 400_000)

    # -- helpers ---------------------------------------------------------------------------

    def _event(self, t: datetime, et: EventType, payload: BaseModel, source: str) -> SimEvent:
        return SimEvent(t, _uuid(self.rng), et, self.shipment_id, source, payload)

    def _ping(
        self, t: datetime, p: Point, speed: float, heading: float, status: DriverStatus
    ) -> SimEvent:
        noisy = offset(p, self.rng.gauss(0, 0.03), self.rng.gauss(0, 0.03))
        return self._event(
            t,
            EventType.GPS_PING,
            GpsPingPayload(
                lat=round(noisy.lat, 6),
                lon=round(noisy.lon, 6),
                speed_mph=round(max(0.0, speed), 1),
                heading=round(heading % 360, 1),
                odometer_mi=round(self.odometer, 1),
                driver_status=status,
            ),
            SOURCE_ELD,
        )

    def _ping_gap(self) -> timedelta:
        return timedelta(seconds=self.rng.uniform(self.cfg.min_ping_s, self.cfg.max_ping_s))

    def _dwell(self, facility: Facility) -> timedelta:
        median = facility_dwell_median_min(facility.facility_id)
        return timedelta(minutes=self.rng.lognormvariate(math.log(median), 0.4))

    def _stationary(
        self, start: datetime, end: datetime, at: Point, status: DriverStatus, every_min: float
    ) -> Iterator[SimEvent]:
        t = start + timedelta(minutes=self.rng.uniform(1, every_min))
        while t < end:
            yield self._ping(t, at, 0.0, 0.0, status)
            t += timedelta(minutes=self.rng.uniform(every_min / 2, every_min))

    def _drive(
        self,
        a: Point,
        b: Point,
        start: datetime,
        cruise: float,
        exception_at_fraction: float | None = None,
    ) -> Generator[SimEvent, None, datetime]:
        """Drive the great circle a -> b; returns (via StopIteration) the arrival time."""
        total_gc = haversine_mi(a, b)
        heading = initial_bearing_deg(a, b)
        traveled = 0.0
        t = start
        exception_done = exception_at_fraction is None
        while traveled < total_gc:
            if self.hos_used >= HOS_MAX_DRIVING_HOURS:
                here = interpolate(a, b, traveled / total_gc)
                rest_end = t + timedelta(hours=HOS_REQUIRED_BREAK_HOURS)
                yield self._ping(
                    t + timedelta(seconds=30), here, 0.0, heading, DriverStatus.SLEEPER
                )
                yield from self._stationary(
                    t + timedelta(minutes=1), rest_end, here, DriverStatus.SLEEPER, 30.0
                )
                t = rest_end
                self.hos_used = 0.0
            if not exception_done and traveled / total_gc >= (exception_at_fraction or 0):
                exception_done = True
                here = interpolate(a, b, traveled / total_gc)
                code = self.rng.choice(
                    [ExceptionCode.WEATHER, ExceptionCode.BREAKDOWN, ExceptionCode.TRAFFIC]
                )
                yield self._event(
                    t + timedelta(seconds=10),
                    EventType.SHIPMENT_EXCEPTION,
                    ExceptionPayload(code=code, note=f"{code.value.lower()} delay"),
                    SOURCE_CARRIER,
                )
                end = t + timedelta(hours=self.rng.uniform(1.0, 3.0))
                yield from self._stationary(
                    t + timedelta(seconds=20), end, here, DriverStatus.ON_DUTY, 10.0
                )
                yield self._event(
                    end,
                    EventType.SHIPMENT_EXCEPTION,
                    ExceptionPayload(code=code, note="resolved", resolved=True),
                    SOURCE_CARRIER,
                )
                t = end + timedelta(seconds=5)
            dt = self._ping_gap()
            speed = min(75.0, max(35.0, self.rng.gauss(cruise, 3.0)))
            hours = dt.total_seconds() / 3600
            step = speed / ROAD_CIRCUITY_FACTOR * hours
            traveled = min(total_gc, traveled + step)
            t += dt
            self.hos_used += hours
            self.odometer += step * ROAD_CIRCUITY_FACTOR
            frac = traveled / total_gc
            yield self._ping(t, interpolate(a, b, frac), speed, heading, DriverStatus.DRIVING)
        return t

    # -- the script ------------------------------------------------------------------------

    def events(self) -> Iterator[SimEvent]:
        rng = self.rng
        o, d = self.origin, self.dest
        t = self.created_at

        pickup_start = _round_to(t + timedelta(hours=rng.uniform(1.0, 3.0)), 15)
        pickup_end = pickup_start + timedelta(hours=2)
        planned_depart = pickup_start + timedelta(minutes=90)
        miles = road_miles(o.location, d.location)
        plan_drive_h = miles / PLANNING_SPEED_MPH
        breaks = required_breaks(plan_drive_h, HOS_MAX_DRIVING_HOURS - self.hos_used)
        planned_arrival = planned_depart + timedelta(
            hours=plan_drive_h + breaks * HOS_REQUIRED_BREAK_HOURS
        )
        # Some receivers book tight windows: roughly 1 load in 5 ends up at risk.
        delivery_start = _round_to(planned_arrival + timedelta(hours=rng.uniform(-3.0, 1.5)), 15)
        delivery_start = max(delivery_start, pickup_end + timedelta(hours=1))
        delivery_end = delivery_start + timedelta(hours=2)

        equipment = rng.choices(
            [Equipment.DRY_VAN, Equipment.REEFER, Equipment.FLATBED], weights=[60, 25, 15]
        )[0]
        mode = Mode.FTL if rng.random() < 0.85 else Mode.LTL
        yield self._event(
            t,
            EventType.SHIPMENT_CREATED,
            ShipmentCreatedPayload(
                customer_id=rng.choice(CUSTOMERS).customer_id,
                mode=mode,
                equipment=equipment,
                stops=[
                    StopSpec(
                        stop_sequence=1,
                        facility_id=o.facility_id,
                        stop_type=StopType.PICKUP,
                        appt_start=pickup_start,
                        appt_end=pickup_end,
                    ),
                    StopSpec(
                        stop_sequence=2,
                        facility_id=d.facility_id,
                        stop_type=StopType.DELIVERY,
                        appt_start=delivery_start,
                        appt_end=delivery_end,
                    ),
                ],
            ),
            SOURCE_TMS,
        )

        # Tendering: ~15% of tenders are rejected and re-tendered to another carrier.
        premium = {Equipment.DRY_VAN: 1.0, Equipment.REEFER: 1.15, Equipment.FLATBED: 1.1}
        base_rate = (250 + 2.2 * miles) * premium[equipment]
        tried: set[str] = set()
        while True:
            carrier = rng.choice([c for c in CARRIERS if c.carrier_id not in tried])
            tried.add(carrier.carrier_id)
            t += timedelta(minutes=rng.uniform(2, 10))
            rate = Decimal(f"{base_rate * rng.uniform(0.9, 1.15):.2f}")
            yield self._event(
                t,
                EventType.SHIPMENT_TENDERED,
                TenderedPayload(carrier_id=carrier.carrier_id, rate_usd=rate),
                SOURCE_TMS,
            )
            t += timedelta(minutes=rng.uniform(3, 25))
            if rng.random() < self.cfg.tender_reject_rate and len(tried) < 5:
                yield self._event(
                    t,
                    EventType.TENDER_REJECTED,
                    TenderResponsePayload(carrier_id=carrier.carrier_id, reason="no capacity"),
                    SOURCE_CARRIER,
                )
                continue
            yield self._event(
                t,
                EventType.TENDER_ACCEPTED,
                TenderResponsePayload(carrier_id=carrier.carrier_id),
                SOURCE_CARRIER,
            )
            break

        cruise = rng.uniform(50.0, 65.0)

        # Deadhead from a nearby yard to the pickup, timed to arrive around the appointment.
        yard = offset(o.location, rng.uniform(-25, 25), rng.uniform(-25, 25))
        deadhead_h = road_miles(yard, o.location) / cruise
        target = pickup_start + timedelta(minutes=rng.gauss(0, 25))
        depart_yard = max(t + timedelta(minutes=5), target - timedelta(hours=deadhead_h))
        t = yield from self._drive(yard, o.location, depart_yard, cruise)
        yield self._event(
            t,
            EventType.STOP_ARRIVED,
            StopEventPayload(stop_sequence=1, facility_id=o.facility_id),
            SOURCE_ELD,
        )
        leave = t + self._dwell(o)
        yield from self._stationary(t, leave, o.location, DriverStatus.ON_DUTY, 15.0)
        yield self._event(
            leave,
            EventType.STOP_DEPARTED,
            StopEventPayload(stop_sequence=1, facility_id=o.facility_id),
            SOURCE_ELD,
        )

        exc_at = rng.uniform(0.2, 0.8) if rng.random() < self.cfg.exception_rate else None
        t = yield from self._drive(
            o.location,
            d.location,
            leave + timedelta(seconds=30),
            cruise,
            exception_at_fraction=exc_at,
        )
        yield self._event(
            t,
            EventType.STOP_ARRIVED,
            StopEventPayload(stop_sequence=2, facility_id=d.facility_id),
            SOURCE_ELD,
        )
        leave = t + self._dwell(d)
        yield from self._stationary(t, leave, d.location, DriverStatus.ON_DUTY, 15.0)
        yield self._event(
            leave,
            EventType.STOP_DEPARTED,
            StopEventPayload(stop_sequence=2, facility_id=d.facility_id),
            SOURCE_ELD,
        )
        yield self._event(
            leave + timedelta(minutes=1),
            EventType.SHIPMENT_DELIVERED,
            DeliveredPayload(
                pod_signed_by=rng.choice(
                    ["J. Smith", "A. Garcia", "M. Chen", "R. Patel", "K. Johnson"]
                )
            ),
            SOURCE_CARRIER,
        )


class Fleet:
    """Merges every shipment's script into one event-time-ordered stream."""

    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        lanes = candidate_lanes()
        self.scripts = [ShipmentScript(cfg, i, lanes) for i in range(cfg.shipments)]

    def events(self) -> Iterator[SimEvent]:
        heap: list[tuple[datetime, int, SimEvent, Iterator[SimEvent]]] = []
        for i, script in enumerate(self.scripts):
            gen = script.events()
            first = next(gen)
            heap.append((first.event_time, i, first, gen))
        heapq.heapify(heap)
        while heap:
            t, i, ev, gen = heapq.heappop(heap)
            yield ev
            nxt = next(gen, None)
            if nxt is not None:
                if nxt.event_time < t:  # pragma: no cover - script invariant
                    raise AssertionError("shipment script went back in time")
                heapq.heappush(heap, (nxt.event_time, i, nxt, gen))

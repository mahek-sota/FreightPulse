"""Builders for valid events and realistic lifecycles, shared by unit and integration tests."""

from __future__ import annotations

import itertools
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from freightpulse.domain.enums import DriverStatus, Equipment, EventType, ExceptionCode, Mode
from freightpulse.domain.events import (
    DeliveredPayload,
    EventEnvelope,
    ExceptionPayload,
    GpsPingPayload,
    ParsedEvent,
    ShipmentCreatedPayload,
    StopEventPayload,
    StopSpec,
    TenderedPayload,
    TenderResponsePayload,
    make_envelope,
    parse_event,
)
from freightpulse.domain.geo import Point, initial_bearing_deg, interpolate, road_miles

T0 = datetime(2026, 11, 10, 8, 0, tzinfo=UTC)

# Two real facilities used across tests (must match the reference seed).
DALLAS = ("FAC-DAL-01", Point(32.7357, -96.8770))
MEMPHIS = ("FAC-MEM-01", Point(35.0456, -89.9773))

_counter = itertools.count(1)


def uid() -> uuid.UUID:
    return uuid.uuid4()


@dataclass
class EventFactory:
    shipment_id: str = field(default_factory=lambda: f"SHP-{next(_counter):06d}")
    t0: datetime = T0
    source: str = "test"
    produced_at: datetime | None = None

    def envelope(
        self, event_type: EventType, at: datetime, payload: Any, event_id: uuid.UUID | None = None
    ) -> EventEnvelope:
        return make_envelope(
            event_id=event_id or uid(),
            event_type=event_type,
            shipment_id=self.shipment_id,
            event_time=at,
            produced_at=self.produced_at or at,
            source=self.source,
            payload=payload,
        )

    def parsed(self, env: EventEnvelope) -> ParsedEvent:
        return parse_event(env.to_json_bytes())

    def created(
        self,
        at: datetime | None = None,
        *,
        pickup: tuple[str, Point] = DALLAS,
        delivery: tuple[str, Point] = MEMPHIS,
        pickup_window: tuple[datetime, datetime] | None = None,
        delivery_window: tuple[datetime, datetime] | None = None,
        customer_id: str = "CUS-001",
    ) -> EventEnvelope:
        at = at or self.t0
        pw = pickup_window or (at + timedelta(hours=1), at + timedelta(hours=3))
        dw = delivery_window or (at + timedelta(hours=12), at + timedelta(hours=16))
        payload = ShipmentCreatedPayload(
            customer_id=customer_id,
            mode=Mode.FTL,
            equipment=Equipment.DRY_VAN,
            stops=[
                StopSpec(
                    stop_sequence=1,
                    facility_id=pickup[0],
                    stop_type="PICKUP",  # type: ignore[arg-type]
                    appt_start=pw[0],
                    appt_end=pw[1],
                ),
                StopSpec(
                    stop_sequence=2,
                    facility_id=delivery[0],
                    stop_type="DELIVERY",  # type: ignore[arg-type]
                    appt_start=dw[0],
                    appt_end=dw[1],
                ),
            ],
        )
        return self.envelope(EventType.SHIPMENT_CREATED, at, payload)

    def tendered(
        self, at: datetime, carrier: str = "CAR-001", rate: str = "2450.00"
    ) -> EventEnvelope:
        return self.envelope(
            EventType.SHIPMENT_TENDERED,
            at,
            TenderedPayload(carrier_id=carrier, rate_usd=Decimal(rate)),
        )

    def accepted(self, at: datetime, carrier: str = "CAR-001") -> EventEnvelope:
        return self.envelope(
            EventType.TENDER_ACCEPTED, at, TenderResponsePayload(carrier_id=carrier)
        )

    def rejected(self, at: datetime, carrier: str = "CAR-001") -> EventEnvelope:
        return self.envelope(
            EventType.TENDER_REJECTED,
            at,
            TenderResponsePayload(carrier_id=carrier, reason="no truck"),
        )

    def arrived(self, at: datetime, seq: int, facility: str) -> EventEnvelope:
        return self.envelope(
            EventType.STOP_ARRIVED, at, StopEventPayload(stop_sequence=seq, facility_id=facility)
        )

    def departed(self, at: datetime, seq: int, facility: str) -> EventEnvelope:
        return self.envelope(
            EventType.STOP_DEPARTED, at, StopEventPayload(stop_sequence=seq, facility_id=facility)
        )

    def delivered(self, at: datetime) -> EventEnvelope:
        return self.envelope(
            EventType.SHIPMENT_DELIVERED, at, DeliveredPayload(pod_signed_by="J. Doe")
        )

    def exception(self, at: datetime, *, resolved: bool = False) -> EventEnvelope:
        return self.envelope(
            EventType.SHIPMENT_EXCEPTION,
            at,
            ExceptionPayload(code=ExceptionCode.WEATHER, note="storm", resolved=resolved),
        )

    def ping(
        self,
        at: datetime,
        p: Point,
        *,
        speed: float = 60.0,
        status: DriverStatus = DriverStatus.DRIVING,
        heading: float = 90.0,
        odometer: float = 1000.0,
    ) -> EventEnvelope:
        return self.envelope(
            EventType.GPS_PING,
            at,
            GpsPingPayload(
                lat=p.lat,
                lon=p.lon,
                speed_mph=speed,
                heading=heading,
                odometer_mi=odometer,
                driver_status=status,
            ),
        )

    def full_lifecycle(
        self, *, pings: int = 6, with_exception: bool = False
    ) -> list[EventEnvelope]:
        """A realistic, strictly time-ordered lifecycle Dallas -> Memphis."""
        t = self.t0
        dal_id, dal = DALLAS
        mem_id, mem = MEMPHIS
        events = [
            self.created(t),
            self.tendered(t + timedelta(minutes=5)),
            self.accepted(t + timedelta(minutes=20)),
            self.arrived(t + timedelta(hours=1, minutes=30), 1, dal_id),
            self.departed(t + timedelta(hours=2, minutes=30), 1, dal_id),
        ]
        drive_start = t + timedelta(hours=2, minutes=31)
        miles = road_miles(dal, mem)
        drive_h = miles / 60.0
        heading = initial_bearing_deg(dal, mem)
        for i in range(pings):
            frac = (i + 1) / (pings + 1)
            at = drive_start + timedelta(hours=drive_h * frac)
            events.append(
                self.ping(
                    at, interpolate(dal, mem, frac), heading=heading, odometer=1000 + miles * frac
                )
            )
            if with_exception and i == pings // 2:
                events.append(self.exception(at + timedelta(minutes=1)))
                events.append(self.exception(at + timedelta(minutes=30), resolved=True))
        arrive = drive_start + timedelta(hours=drive_h + 0.2)
        events += [
            self.arrived(arrive, 2, mem_id),
            self.departed(arrive + timedelta(hours=1), 2, mem_id),
            self.delivered(arrive + timedelta(hours=1, minutes=1)),
        ]
        return events


FACILITY_LOCATIONS = {DALLAS[0]: DALLAS[1], MEMPHIS[0]: MEMPHIS[1]}

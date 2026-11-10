"""Pydantic response models for the public API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class ErrorDetail(BaseModel):
    code: str = Field(examples=["shipment_not_found"])
    message: str
    details: Any | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class Health(BaseModel):
    status: str = "ok"


class Readiness(BaseModel):
    status: str
    postgres: bool
    broker: bool


class StopOut(BaseModel):
    stop_sequence: int
    facility_id: str
    facility_name: str
    city: str
    state: str
    lat: float
    lon: float
    stop_type: str
    appt_start: datetime
    appt_end: datetime
    arrived_at: datetime | None
    departed_at: datetime | None
    dwell_minutes: float | None = None


class ShipmentSummary(BaseModel):
    shipment_id: str
    status: str
    customer_id: str
    carrier_id: str | None
    lane_id: int
    origin_facility_id: str
    dest_facility_id: str
    late_risk: str
    eta: datetime | None
    miles_remaining: float | None
    last_ping_at: datetime | None
    updated_at: datetime


class ShipmentPage(BaseModel):
    items: list[ShipmentSummary]
    next_cursor: str | None = Field(
        description="Opaque keyset cursor; pass it back as ?cursor= for the next page."
    )
    limit: int


class EtaOut(BaseModel):
    eta: datetime | None
    eta_confidence: float | None = Field(description="Confidence as stored at the last ping")
    effective_confidence: float | None = Field(
        description="Confidence decayed by the age of the last ping (fleet clock)"
    )
    miles_remaining: float | None
    next_stop_sequence: int | None
    late_risk: str


class ShipmentDetail(BaseModel):
    shipment_id: str
    status: str
    pre_exception_status: str | None
    mode: str
    equipment: str
    customer_id: str
    customer_name: str
    carrier_id: str | None
    carrier_name: str | None
    rate_usd: Decimal | None
    lane_id: int
    lane_distance_mi: float
    last_lat: float | None
    last_lon: float | None
    last_ping_at: datetime | None
    last_event_time: datetime
    eta: EtaOut
    stops: list[StopOut]
    active_alerts: int
    version: int
    created_at: datetime
    updated_at: datetime


class TimelineEvent(BaseModel):
    event_id: UUID
    event_type: str
    event_time: datetime
    ingested_at: datetime
    source: str
    payload: dict[str, Any]


class Timeline(BaseModel):
    shipment_id: str
    events: list[TimelineEvent]


class TrackPoint(BaseModel):
    event_time: datetime
    lat: float
    lon: float
    speed_mph: float
    driver_status: str


class Track(BaseModel):
    shipment_id: str
    points: list[TrackPoint]


class AlertOut(BaseModel):
    alert_id: UUID
    shipment_id: str
    alert_type: str
    severity: str
    message: str
    triggered_at: datetime
    resolved_at: datetime | None
    dedupe_key: str
    details: dict[str, Any]


class AlertList(BaseModel):
    items: list[AlertOut]


class LaneMetricsRow(BaseModel):
    hour_bucket: datetime
    shipments_delivered: int
    on_time_pct: Decimal | None
    avg_transit_hours: Decimal | None
    p90_transit_hours: Decimal | None
    avg_detention_min: Decimal | None


class LaneMetrics(BaseModel):
    lane_id: int
    origin_facility_id: str
    dest_facility_id: str
    distance_mi: float
    rows: list[LaneMetricsRow]


class ScorecardDay(BaseModel):
    day: date
    tenders: int
    acceptance_rate: Decimal | None
    on_time_pickup_pct: Decimal | None
    on_time_delivery_pct: Decimal | None
    tracking_compliance_pct: Decimal | None


class Scorecard(BaseModel):
    carrier_id: str
    name: str
    scac: str
    days: int
    tenders: int
    acceptance_rate: float | None = Field(description="Tender-weighted over the period")
    on_time_pickup_pct: float | None
    on_time_delivery_pct: float | None
    tracking_compliance_pct: float | None
    daily: list[ScorecardDay]


class FleetSummary(BaseModel):
    fleet_time: datetime = Field(description="Event-time clock (newest event processed)")
    shipments_total: int
    in_transit: int
    at_risk: int = Field(description="Undelivered shipments with MEDIUM or HIGH late risk")
    by_status: dict[str, int]
    active_alerts: dict[str, int]
    detained_now: int
    detention_cost_today_usd: float
    delivered_today: int

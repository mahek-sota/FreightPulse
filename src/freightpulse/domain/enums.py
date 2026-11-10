"""Domain enumerations shared by every layer."""

from __future__ import annotations

from enum import StrEnum


class EventType(StrEnum):
    SHIPMENT_CREATED = "shipment.created"
    SHIPMENT_TENDERED = "shipment.tendered"
    TENDER_ACCEPTED = "shipment.tender_accepted"
    TENDER_REJECTED = "shipment.tender_rejected"
    STOP_ARRIVED = "stop.arrived"
    STOP_DEPARTED = "stop.departed"
    SHIPMENT_DELIVERED = "shipment.delivered"
    SHIPMENT_EXCEPTION = "shipment.exception"
    GPS_PING = "gps.ping"

    @property
    def is_telemetry(self) -> bool:
        return self is EventType.GPS_PING


class ShipmentStatus(StrEnum):
    CREATED = "CREATED"
    TENDERED = "TENDERED"
    REJECTED = "REJECTED"
    ACCEPTED = "ACCEPTED"
    AT_PICKUP = "AT_PICKUP"
    IN_TRANSIT = "IN_TRANSIT"
    AT_DELIVERY = "AT_DELIVERY"
    DELIVERED = "DELIVERED"
    EXCEPTION = "EXCEPTION"


class StopType(StrEnum):
    PICKUP = "PICKUP"
    DELIVERY = "DELIVERY"


class Mode(StrEnum):
    FTL = "FTL"
    LTL = "LTL"


class Equipment(StrEnum):
    DRY_VAN = "dry_van"
    REEFER = "reefer"
    FLATBED = "flatbed"


class DriverStatus(StrEnum):
    DRIVING = "DRIVING"
    ON_DUTY = "ON_DUTY"
    OFF_DUTY = "OFF_DUTY"
    SLEEPER = "SLEEPER"

    @property
    def is_rest(self) -> bool:
        return self in (DriverStatus.OFF_DUTY, DriverStatus.SLEEPER)


class ExceptionCode(StrEnum):
    WEATHER = "WEATHER"
    BREAKDOWN = "BREAKDOWN"
    RECEIVER_CLOSED = "RECEIVER_CLOSED"
    TRAFFIC = "TRAFFIC"
    ACCIDENT = "ACCIDENT"
    PAPERWORK = "PAPERWORK"
    OTHER = "OTHER"


class LateRisk(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class AlertType(StrEnum):
    LATE_RISK = "LATE_RISK"
    DETENTION = "DETENTION"
    STALE_TRACKING = "STALE_TRACKING"


class Severity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

    @property
    def rank(self) -> int:
        return _SEVERITY_RANK[self]


_SEVERITY_RANK = {Severity.LOW: 0, Severity.MEDIUM: 1, Severity.HIGH: 2, Severity.CRITICAL: 3}


class AlertAction(StrEnum):
    TRIGGER = "TRIGGER"
    RESOLVE = "RESOLVE"


class DlqErrorType(StrEnum):
    SCHEMA_VALIDATION = "SCHEMA_VALIDATION"
    UNKNOWN_SHIPMENT = "UNKNOWN_SHIPMENT"
    UNKNOWN_REFERENCE = "UNKNOWN_REFERENCE"
    INVALID_TRANSITION = "INVALID_TRANSITION"
    IMPOSSIBLE_GPS_JUMP = "IMPOSSIBLE_GPS_JUMP"

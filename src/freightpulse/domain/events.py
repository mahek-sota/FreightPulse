"""Event contracts: the envelope every event carries plus a typed payload per event type.

Producers serialise ``EventEnvelope`` to JSON; consumers call :func:`parse_event`, which
validates the envelope *and* the event-type-specific payload, raising
:class:`EventValidationError` (routed to the DLQ as ``SCHEMA_VALIDATION``).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Final, cast
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from freightpulse.domain.enums import (
    DriverStatus,
    Equipment,
    EventType,
    ExceptionCode,
    Mode,
    StopType,
)

CURRENT_SCHEMA_VERSION: Final = 1
SUPPORTED_SCHEMA_VERSIONS: Final = frozenset({1})
MAX_CLOCK_SKEW: Final = timedelta(days=30)
"""|event_time - produced_at| larger than this is treated as a corrupt timestamp.

It is generous because the simulator runs an accelerated clock (60x by default), so simulated
event times legitimately run ahead of the wall-clock ``produced_at``."""

SHIPMENT_ID_PATTERN: Final = r"^SHP-\d{6,}$"


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- payloads ----------------------------------------------------------------------------


class StopSpec(_Strict):
    stop_sequence: Annotated[int, Field(ge=1)]
    facility_id: Annotated[str, Field(min_length=1, max_length=32)]
    stop_type: StopType
    appt_start: AwareDatetime
    appt_end: AwareDatetime

    @model_validator(mode="after")
    def _window(self) -> StopSpec:
        if self.appt_end < self.appt_start:
            raise ValueError("appt_end must not be before appt_start")
        return self


class ShipmentCreatedPayload(_Strict):
    customer_id: Annotated[str, Field(min_length=1, max_length=32)]
    mode: Mode
    equipment: Equipment
    stops: Annotated[list[StopSpec], Field(min_length=2, max_length=20)]

    @field_validator("stops")
    @classmethod
    def _stops_are_well_formed(cls, stops: list[StopSpec]) -> list[StopSpec]:
        seqs = [s.stop_sequence for s in stops]
        if seqs != list(range(1, len(stops) + 1)):
            raise ValueError("stop_sequence values must be 1..n in order")
        if stops[0].stop_type is not StopType.PICKUP:
            raise ValueError("first stop must be a PICKUP")
        if stops[-1].stop_type is not StopType.DELIVERY:
            raise ValueError("last stop must be a DELIVERY")
        return stops


class TenderedPayload(_Strict):
    carrier_id: Annotated[str, Field(min_length=1, max_length=32)]
    rate_usd: Annotated[Decimal, Field(gt=0, max_digits=12, decimal_places=2)]


class TenderResponsePayload(_Strict):
    carrier_id: Annotated[str, Field(min_length=1, max_length=32)]
    reason: str | None = None


class StopEventPayload(_Strict):
    stop_sequence: Annotated[int, Field(ge=1)]
    facility_id: Annotated[str, Field(min_length=1, max_length=32)]


class DeliveredPayload(_Strict):
    pod_signed_by: Annotated[str, Field(min_length=1, max_length=128)]


class ExceptionPayload(_Strict):
    code: ExceptionCode
    note: str = ""
    resolved: bool = False
    """``True`` marks the end of the exception; status returns to the prior active state."""


class GpsPingPayload(_Strict):
    lat: Annotated[float, Field(ge=-90, le=90, allow_inf_nan=False)]
    lon: Annotated[float, Field(ge=-180, le=180, allow_inf_nan=False)]
    speed_mph: Annotated[float, Field(ge=0, le=150, allow_inf_nan=False)]
    heading: Annotated[float, Field(ge=0, lt=360, allow_inf_nan=False)]
    odometer_mi: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    driver_status: DriverStatus


Payload = (
    ShipmentCreatedPayload
    | TenderedPayload
    | TenderResponsePayload
    | StopEventPayload
    | DeliveredPayload
    | ExceptionPayload
    | GpsPingPayload
)

PAYLOAD_MODELS: Final[dict[EventType, type[_Strict]]] = {
    EventType.SHIPMENT_CREATED: ShipmentCreatedPayload,
    EventType.SHIPMENT_TENDERED: TenderedPayload,
    EventType.TENDER_ACCEPTED: TenderResponsePayload,
    EventType.TENDER_REJECTED: TenderResponsePayload,
    EventType.STOP_ARRIVED: StopEventPayload,
    EventType.STOP_DEPARTED: StopEventPayload,
    EventType.SHIPMENT_DELIVERED: DeliveredPayload,
    EventType.SHIPMENT_EXCEPTION: ExceptionPayload,
    EventType.GPS_PING: GpsPingPayload,
}


# --- envelope ----------------------------------------------------------------------------


class EventEnvelope(BaseModel):
    """The wire contract shared by every event (spec section 4.1)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: UUID
    event_type: EventType
    schema_version: int = CURRENT_SCHEMA_VERSION
    shipment_id: Annotated[str, Field(pattern=SHIPMENT_ID_PATTERN)]
    event_time: AwareDatetime
    produced_at: AwareDatetime
    source: Annotated[str, Field(min_length=1, max_length=64)]
    payload: dict[str, Any]

    @field_validator("event_time", "produced_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return _to_utc(value)

    @field_validator("schema_version")
    @classmethod
    def _supported_version(cls, value: int) -> int:
        if value not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version {value}")
        return value

    @model_validator(mode="after")
    def _sane_clock(self) -> EventEnvelope:
        if abs(self.event_time - self.produced_at) > MAX_CLOCK_SKEW:
            raise ValueError("event_time is implausibly far from produced_at")
        return self

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class ParsedEvent(BaseModel):
    """A fully validated event: envelope plus its typed payload."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    envelope: EventEnvelope
    payload: Payload

    @property
    def event_id(self) -> UUID:
        return self.envelope.event_id

    @property
    def event_type(self) -> EventType:
        return self.envelope.event_type

    @property
    def shipment_id(self) -> str:
        return self.envelope.shipment_id

    @property
    def event_time(self) -> datetime:
        return self.envelope.event_time


class EventValidationError(Exception):
    """Raised when raw bytes are not a valid event. ``detail`` is safe to put in the DLQ."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors(include_url=False)[:5]:
        loc = ".".join(str(p) for p in err["loc"]) or "<root>"
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts)


def validate_payload(event_type: EventType, payload: dict[str, Any]) -> Payload:
    model = PAYLOAD_MODELS[event_type]
    try:
        parsed = model.model_validate(payload)
    except ValidationError as exc:
        raise EventValidationError(f"payload: {_format_validation_error(exc)}") from exc
    return cast(Payload, parsed)


def parse_event(raw: bytes | str) -> ParsedEvent:
    """Validate raw JSON into a :class:`ParsedEvent` or raise :class:`EventValidationError`."""
    try:
        envelope = EventEnvelope.model_validate_json(raw)
    except ValidationError as exc:
        raise EventValidationError(f"envelope: {_format_validation_error(exc)}") from exc
    payload = validate_payload(envelope.event_type, envelope.payload)
    return ParsedEvent(envelope=envelope, payload=payload)


def make_envelope(
    *,
    event_id: UUID,
    event_type: EventType,
    shipment_id: str,
    event_time: datetime,
    produced_at: datetime,
    source: str,
    payload: BaseModel,
) -> EventEnvelope:
    """Build an envelope from a typed payload (used by the simulator and tests)."""
    return EventEnvelope(
        event_id=event_id,
        event_type=event_type,
        shipment_id=shipment_id,
        event_time=event_time,
        produced_at=produced_at,
        source=source,
        payload=payload.model_dump(mode="json"),
    )

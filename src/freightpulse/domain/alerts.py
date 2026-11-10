"""Alert rules and the alert message contract published to ``freight.alerts``.

Every alert carries a deterministic ``dedupe_key`` and ``alert_id = uuid5(dedupe_key)`` so that
re-emitting the same alert (after a crash, a retry or a rescan) is idempotent at the sink.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Final
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from freightpulse.domain.enums import AlertAction, AlertType, LateRisk, Severity

ALERT_NAMESPACE: Final = UUID("5b0f6f0e-2a8f-4b8e-9a51-6c1f2f6d9a10")

DETENTION_FREE_TIME: Final = timedelta(hours=2)
DETENTION_RATE_USD_PER_HOUR: Final = 75.0
STALE_TRACKING_AFTER: Final = timedelta(minutes=60)


class AlertMessage(BaseModel):
    model_config = ConfigDict(frozen=True)

    alert_id: UUID
    dedupe_key: str
    shipment_id: str
    alert_type: AlertType
    severity: Severity
    action: AlertAction = AlertAction.TRIGGER
    message: str
    triggered_at: AwareDatetime
    """Event time (fleet clock) at which the condition was detected."""
    details: dict[str, Any] = Field(default_factory=dict)
    produced_at: AwareDatetime | None = None

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode()


def alert_id_for(dedupe_key: str) -> UUID:
    return uuid5(ALERT_NAMESPACE, dedupe_key)


def _alert(
    *,
    key: str,
    shipment_id: str,
    alert_type: AlertType,
    severity: Severity,
    message: str,
    at: datetime,
    details: dict[str, Any],
    action: AlertAction = AlertAction.TRIGGER,
) -> AlertMessage:
    return AlertMessage(
        alert_id=alert_id_for(key),
        dedupe_key=key,
        shipment_id=shipment_id,
        alert_type=alert_type,
        severity=severity,
        action=action,
        message=message,
        triggered_at=at,
        details=details,
    )


# --- late risk ---------------------------------------------------------------------------

_LATE_SEVERITY: Final = {
    LateRisk.LOW: Severity.LOW,
    LateRisk.MEDIUM: Severity.MEDIUM,
    LateRisk.HIGH: Severity.HIGH,
}


def late_risk_alert(
    *,
    shipment_id: str,
    stop_sequence: int,
    previous: LateRisk,
    level: LateRisk,
    eta: datetime | None,
    appt_end: datetime | None,
    trigger_event_id: UUID,
    at: datetime,
) -> AlertMessage | None:
    """Alert only on a *level change*. A drop back to LOW resolves the active alert."""
    if level is previous:
        return None
    # The trigger event id makes the key unique per level change yet stable across replays.
    key = f"late_risk:{shipment_id}:{stop_sequence}:{level}:{trigger_event_id}"
    details: dict[str, Any] = {
        "stop_sequence": stop_sequence,
        "previous_level": previous.value,
        "level": level.value,
        "eta": eta.isoformat() if eta else None,
        "appt_end": appt_end.isoformat() if appt_end else None,
    }
    if level is LateRisk.LOW:
        return _alert(
            key=key,
            shipment_id=shipment_id,
            alert_type=AlertType.LATE_RISK,
            severity=Severity.LOW,
            message=f"{shipment_id} back on time for stop {stop_sequence}",
            at=at,
            details=details,
            action=AlertAction.RESOLVE,
        )
    if eta is not None and appt_end is not None:
        delta_min = (eta - appt_end).total_seconds() / 60
        when = f"{abs(delta_min):.0f} min {'after' if delta_min > 0 else 'before'} window end"
    else:  # pragma: no cover - defensive
        when = "unknown ETA"
    return _alert(
        key=key,
        shipment_id=shipment_id,
        alert_type=AlertType.LATE_RISK,
        severity=_LATE_SEVERITY[level],
        message=f"{shipment_id} {level} late risk at stop {stop_sequence}: ETA {when}",
        at=at,
        details=details,
    )


def late_risk_cleared(
    *, shipment_id: str, stop_sequence: int, reason: str, at: datetime
) -> AlertMessage:
    """Resolve any active late-risk alert (the stop was reached or the load delivered)."""
    key = f"late_risk:{shipment_id}:{stop_sequence}:CLEARED"
    return _alert(
        key=key,
        shipment_id=shipment_id,
        alert_type=AlertType.LATE_RISK,
        severity=Severity.LOW,
        message=f"{shipment_id} late risk cleared: {reason}",
        at=at,
        details={"stop_sequence": stop_sequence, "reason": reason},
        action=AlertAction.RESOLVE,
    )


# --- detention ---------------------------------------------------------------------------


def detention_severity(dwell: timedelta) -> Severity | None:
    """None inside the 2h free time; MEDIUM after 2h, HIGH after 3h, CRITICAL after 4h."""
    hours = dwell.total_seconds() / 3600
    if hours < 2:
        return None
    if hours < 3:
        return Severity.MEDIUM
    if hours < 4:
        return Severity.HIGH
    return Severity.CRITICAL


def detention_cost_usd(dwell: timedelta) -> float:
    billable_h = max(0.0, (dwell - DETENTION_FREE_TIME).total_seconds() / 3600)
    return round(billable_h * DETENTION_RATE_USD_PER_HOUR, 2)


def detention_key(shipment_id: str, stop_sequence: int) -> str:
    return f"detention:{shipment_id}:{stop_sequence}"


def detention_alert(
    *,
    shipment_id: str,
    stop_sequence: int,
    facility_id: str,
    arrived_at: datetime,
    now: datetime,
) -> AlertMessage | None:
    dwell = now - arrived_at
    severity = detention_severity(dwell)
    if severity is None:
        return None
    cost = detention_cost_usd(dwell)
    hours = dwell.total_seconds() / 3600
    return _alert(
        key=detention_key(shipment_id, stop_sequence),
        shipment_id=shipment_id,
        alert_type=AlertType.DETENTION,
        severity=severity,
        message=(
            f"{shipment_id} detained {hours:.1f}h at {facility_id} (stop {stop_sequence}); "
            f"est. detention ${cost:,.2f}"
        ),
        at=now,
        details={
            "stop_sequence": stop_sequence,
            "facility_id": facility_id,
            "arrived_at": arrived_at.isoformat(),
            "dwell_hours": round(hours, 2),
            "detention_cost_usd": cost,
        },
    )


def detention_resolved(
    *, shipment_id: str, stop_sequence: int, departed_at: datetime, dwell: timedelta
) -> AlertMessage:
    return _alert(
        key=detention_key(shipment_id, stop_sequence),
        shipment_id=shipment_id,
        alert_type=AlertType.DETENTION,
        severity=Severity.LOW,
        message=f"{shipment_id} departed stop {stop_sequence}",
        at=departed_at,
        details={
            "stop_sequence": stop_sequence,
            "dwell_hours": round(dwell.total_seconds() / 3600, 2),
            "detention_cost_usd": detention_cost_usd(dwell),
        },
        action=AlertAction.RESOLVE,
    )


# --- stale tracking ----------------------------------------------------------------------


def stale_key(shipment_id: str, last_ping_at: datetime) -> str:
    return f"stale:{shipment_id}:{last_ping_at.strftime('%Y%m%dT%H%M%S')}"


def is_stale(last_ping_at: datetime | None, now: datetime) -> bool:
    return last_ping_at is not None and now - last_ping_at > STALE_TRACKING_AFTER


def stale_tracking_alert(
    *, shipment_id: str, last_ping_at: datetime, now: datetime
) -> AlertMessage | None:
    silent = now - last_ping_at
    if silent <= STALE_TRACKING_AFTER:
        return None
    minutes = silent.total_seconds() / 60
    severity = Severity.MEDIUM if minutes < 180 else Severity.HIGH
    return _alert(
        key=stale_key(shipment_id, last_ping_at),
        shipment_id=shipment_id,
        alert_type=AlertType.STALE_TRACKING,
        severity=severity,
        message=f"{shipment_id} has not reported GPS for {minutes:.0f} min while IN_TRANSIT",
        at=now,
        details={"last_ping_at": last_ping_at.isoformat(), "silent_minutes": round(minutes, 1)},
    )


def stale_tracking_resolved(
    *, shipment_id: str, last_ping_at: datetime, at: datetime, reason: str
) -> AlertMessage:
    return _alert(
        key=stale_key(shipment_id, last_ping_at),
        shipment_id=shipment_id,
        alert_type=AlertType.STALE_TRACKING,
        severity=Severity.LOW,
        message=f"{shipment_id} tracking restored ({reason})",
        at=at,
        details={"reason": reason},
        action=AlertAction.RESOLVE,
    )

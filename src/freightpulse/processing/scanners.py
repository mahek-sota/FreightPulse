"""Periodic alert scanners: detention and stale tracking (spec 6.3 #3 and #4).

Both run against the **fleet clock**: the newest event time any processor partition has seen
(``stream_watermarks``). Event time, not wall time, is what matters: the simulator runs at 60x
and a replay of yesterday's data must produce yesterday's alerts.

A scanner emits an alert only when it is new or its severity increased; re-emitting the same
alert is harmless because the sink upserts by ``dedupe_key``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Final

from sqlalchemy import Connection, Engine, text

from freightpulse.domain.alerts import (
    DETENTION_FREE_TIME,
    STALE_TRACKING_AFTER,
    AlertMessage,
    detention_alert,
    detention_resolved,
    stale_key,
    stale_tracking_alert,
    stale_tracking_resolved,
)
from freightpulse.domain.enums import Severity
from freightpulse.storage.repositories import fleet_clock

log = logging.getLogger("scanners")

_RANK: Final = {s.value: s.rank for s in Severity}


def current_fleet_time(conn: Connection) -> datetime:
    return fleet_clock(conn) or datetime.now(UTC)


def _active_severity(conn: Connection, keys: list[str]) -> dict[str, str]:
    if not keys:
        return {}
    rows = conn.execute(
        text("SELECT dedupe_key, severity, resolved_at FROM alerts WHERE dedupe_key = ANY(:keys)"),
        {"keys": keys},
    ).all()
    # A resolved alert never re-triggers under the same key.
    return {r.dedupe_key: ("RESOLVED" if r.resolved_at else r.severity) for r in rows}


def _escalations(conn: Connection, candidates: list[AlertMessage]) -> list[AlertMessage]:
    known = _active_severity(conn, [a.dedupe_key for a in candidates])
    out = []
    for a in candidates:
        prev = known.get(a.dedupe_key)
        if prev == "RESOLVED":
            continue
        if prev is None or a.severity.rank > _RANK[prev]:
            out.append(a)
    return out


def scan_detention(conn: Connection, now: datetime) -> list[AlertMessage]:
    """Trucks sitting at a stop past the 2h free time; resolve when they have left."""
    rows = conn.execute(
        text(
            """
            SELECT st.shipment_id, st.stop_sequence, st.facility_id, st.arrived_at
            FROM shipment_stops st
            WHERE st.arrived_at IS NOT NULL AND st.departed_at IS NULL
              AND st.arrived_at <= :cutoff
            """
        ),
        {"cutoff": now - DETENTION_FREE_TIME},
    ).all()
    candidates = [
        a
        for r in rows
        if (
            a := detention_alert(
                shipment_id=r.shipment_id,
                stop_sequence=r.stop_sequence,
                facility_id=r.facility_id,
                arrived_at=r.arrived_at,
                now=now,
            )
        )
        is not None
    ]
    alerts = _escalations(conn, candidates)

    departed = conn.execute(
        text(
            """
            SELECT a.shipment_id, (a.details->>'stop_sequence')::int AS stop_sequence,
                   st.arrived_at, st.departed_at
            FROM alerts a
            JOIN shipment_stops st
              ON st.shipment_id = a.shipment_id
             AND st.stop_sequence = (a.details->>'stop_sequence')::int
            WHERE a.alert_type = 'DETENTION' AND a.resolved_at IS NULL
              AND st.departed_at IS NOT NULL
            """
        )
    ).all()
    alerts.extend(
        detention_resolved(
            shipment_id=r.shipment_id,
            stop_sequence=r.stop_sequence,
            departed_at=r.departed_at,
            dwell=r.departed_at - r.arrived_at,
        )
        for r in departed
    )
    return alerts


def scan_stale_tracking(conn: Connection, now: datetime) -> list[AlertMessage]:
    """IN_TRANSIT shipments silent for more than 60 minutes; resolve when pings resume."""
    rows = conn.execute(
        text(
            """
            SELECT shipment_id, last_ping_at FROM shipment_current_state
            WHERE status = 'IN_TRANSIT' AND last_ping_at < :cutoff
            """
        ),
        {"cutoff": now - STALE_TRACKING_AFTER},
    ).all()
    candidates = [
        a
        for r in rows
        if (
            a := stale_tracking_alert(
                shipment_id=r.shipment_id, last_ping_at=r.last_ping_at, now=now
            )
        )
        is not None
    ]
    alerts = _escalations(conn, candidates)

    recovered = conn.execute(
        text(
            """
            SELECT a.shipment_id, (a.details->>'last_ping_at')::timestamptz AS silent_since,
                   c.last_ping_at, c.status
            FROM alerts a JOIN shipment_current_state c USING (shipment_id)
            WHERE a.alert_type = 'STALE_TRACKING' AND a.resolved_at IS NULL
              AND (c.status <> 'IN_TRANSIT'
                   OR c.last_ping_at > (a.details->>'last_ping_at')::timestamptz)
            """
        )
    ).all()
    for r in recovered:
        key_ts = r.silent_since
        reason = "GPS resumed" if r.status == "IN_TRANSIT" else f"status {r.status}"
        resolve = stale_tracking_resolved(
            shipment_id=r.shipment_id, last_ping_at=key_ts, at=r.last_ping_at or now, reason=reason
        )
        assert resolve.dedupe_key == stale_key(r.shipment_id, key_ts)
        alerts.append(resolve)
    return alerts


def run_scanners(
    engine: Engine, now: datetime | None = None
) -> tuple[datetime, list[AlertMessage]]:
    with engine.connect() as conn:
        clock = now or current_fleet_time(conn)
        alerts = scan_detention(conn, clock) + scan_stale_tracking(conn, clock)
    return clock, alerts

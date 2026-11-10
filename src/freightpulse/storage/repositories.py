"""Database access for the stream processor (SQLAlchemy Core + raw SQL where it is clearer).

All functions take an open ``Connection`` and never commit: the processor owns the transaction
boundary (one transaction per batch, committed before Kafka offsets; see ADR 0002).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, text
from sqlalchemy.dialects.postgresql import insert

from freightpulse.domain.enums import DriverStatus, LateRisk, ShipmentStatus, StopType
from freightpulse.domain.eta import HosClock, SpeedStats
from freightpulse.domain.geo import Point
from freightpulse.domain.shipment_state import ShipmentState, StopState
from freightpulse.storage import tables as t


class VersionConflictError(Exception):
    """Another writer updated a shipment's state since we read it (optimistic concurrency)."""


@dataclass(frozen=True, slots=True)
class ReferenceSnapshot:
    facilities: dict[str, Point]
    customers: frozenset[str]
    carriers: frozenset[str]


def load_reference(conn: Connection) -> ReferenceSnapshot:
    fac = {r.facility_id: Point(r.lat, r.lon) for r in conn.execute(t.facilities.select())}
    cus = frozenset(conn.execute(text("SELECT customer_id FROM customers")).scalars())
    car = frozenset(conn.execute(text("SELECT carrier_id FROM carriers")).scalars())
    return ReferenceSnapshot(fac, cus, car)


# --- reads -------------------------------------------------------------------------------------


def existing_event_ids(
    conn: Connection, table: str, ids: Sequence[UUID], tmin: datetime, tmax: datetime
) -> set[UUID]:
    """Event ids already in the log. The time range lets Postgres prune partitions."""
    if not ids:
        return set()
    if table not in ("shipment_events", "gps_pings"):
        raise ValueError(table)
    rows = conn.execute(
        text(
            f"SELECT event_id FROM {table} "
            "WHERE event_id = ANY(:ids) AND event_time BETWEEN :tmin AND :tmax"
        ),
        {"ids": list(ids), "tmin": tmin, "tmax": tmax},
    ).scalars()
    return set(rows)


def load_states(
    conn: Connection, shipment_ids: Iterable[str], facilities: dict[str, Point]
) -> dict[str, ShipmentState]:
    ids = sorted(set(shipment_ids))
    if not ids:
        return {}
    stops: dict[str, list[StopState]] = {}
    for r in conn.execute(
        text(
            "SELECT shipment_id, stop_sequence, facility_id, stop_type, appt_start, appt_end, "
            "arrived_at, departed_at FROM shipment_stops WHERE shipment_id = ANY(:ids) "
            "ORDER BY shipment_id, stop_sequence"
        ),
        {"ids": ids},
    ):
        stops.setdefault(r.shipment_id, []).append(
            StopState(
                stop_sequence=r.stop_sequence,
                facility_id=r.facility_id,
                stop_type=StopType(r.stop_type),
                location=facilities[r.facility_id],
                appt_start=r.appt_start,
                appt_end=r.appt_end,
                arrived_at=r.arrived_at,
                departed_at=r.departed_at,
            )
        )
    out: dict[str, ShipmentState] = {}
    for r in conn.execute(
        text(
            "SELECT c.*, s.carrier_id, s.rate_usd FROM shipment_current_state c "
            "JOIN shipments s USING (shipment_id) WHERE c.shipment_id = ANY(:ids)"
        ),
        {"ids": ids},
    ):
        out[r.shipment_id] = ShipmentState(
            shipment_id=r.shipment_id,
            status=ShipmentStatus(r.status),
            status_event_time=r.status_event_time,
            stops=tuple(stops.get(r.shipment_id, ())),
            last_event_time=r.last_event_time,
            pre_exception_status=(
                ShipmentStatus(r.pre_exception_status) if r.pre_exception_status else None
            ),
            carrier_id=r.carrier_id,
            carrier_event_time=r.carrier_event_time,
            rate_usd=r.rate_usd,
            rate_event_time=r.rate_event_time,
            last_lat=r.last_lat,
            last_lon=r.last_lon,
            last_ping_at=r.last_ping_at,
            speed=SpeedStats(mean=r.ewma_speed_mph, var=r.speed_var, samples=r.speed_samples),
            hos=HosClock(
                drive_hours_used=r.hos_drive_hours,
                rest_started_at=r.hos_rest_started_at,
                last_status=DriverStatus(r.hos_last_status) if r.hos_last_status else None,
            ),
            next_stop_sequence=r.next_stop_sequence,
            eta=r.eta,
            eta_confidence=r.eta_confidence,
            miles_remaining=float(r.miles_remaining) if r.miles_remaining is not None else None,
            late_risk=LateRisk(r.late_risk),
            late_risk_stop_sequence=r.late_risk_stop_sequence,
            suspect_lat=r.suspect_lat,
            suspect_lon=r.suspect_lon,
            suspect_at=r.suspect_at,
            version=r.version,
        )
    return out


# --- writes ------------------------------------------------------------------------------------


def upsert_lane(conn: Connection, origin: str, dest: str, distance_mi: float) -> int:
    ins = insert(t.lanes).values(
        origin_facility_id=origin, dest_facility_id=dest, distance_mi=round(distance_mi, 1)
    )
    upsert = ins.on_conflict_do_update(
        index_elements=["origin_facility_id", "dest_facility_id"],
        set_={"distance_mi": ins.excluded.distance_mi},
    ).returning(t.lanes.c.lane_id)
    return int(conn.execute(upsert).scalar_one())


def _state_row(s: ShipmentState) -> dict[str, Any]:
    return {
        "shipment_id": s.shipment_id,
        "status": s.status.value,
        "last_lat": s.last_lat,
        "last_lon": s.last_lon,
        "last_ping_at": s.last_ping_at,
        "next_stop_sequence": s.next_stop_sequence,
        "eta": s.eta,
        "eta_confidence": s.eta_confidence,
        "late_risk": s.late_risk.value,
        "miles_remaining": s.miles_remaining,
        "last_event_time": s.last_event_time,
        "status_event_time": s.status_event_time,
        "pre_exception_status": s.pre_exception_status.value if s.pre_exception_status else None,
        "carrier_event_time": s.carrier_event_time,
        "rate_event_time": s.rate_event_time,
        "ewma_speed_mph": s.speed.mean,
        "speed_var": s.speed.var,
        "speed_samples": s.speed.samples,
        "hos_drive_hours": s.hos.drive_hours_used,
        "hos_rest_started_at": s.hos.rest_started_at,
        "hos_last_status": s.hos.last_status.value if s.hos.last_status else None,
        "late_risk_stop_sequence": s.late_risk_stop_sequence,
        "suspect_lat": s.suspect_lat,
        "suspect_lon": s.suspect_lon,
        "suspect_at": s.suspect_at,
    }


def insert_new_shipments(
    conn: Connection,
    created: Sequence[tuple[ShipmentState, dict[str, Any]]],
) -> None:
    """Insert shipments, their stops and initial state. ``created`` pairs state with
    {customer_id, mode, equipment, lane_id, created_at}."""
    if not created:
        return
    conn.execute(
        insert(t.shipments),
        [
            {
                "shipment_id": s.shipment_id,
                "customer_id": meta["customer_id"],
                "carrier_id": s.carrier_id,
                "lane_id": meta["lane_id"],
                "mode": meta["mode"],
                "equipment": meta["equipment"],
                "status": s.status.value,
                "rate_usd": s.rate_usd,
                "created_at": meta["created_at"],
                "updated_at": meta["created_at"],
            }
            for s, meta in created
        ],
    )
    conn.execute(
        insert(t.shipment_stops),
        [
            {
                "shipment_id": s.shipment_id,
                "stop_sequence": st.stop_sequence,
                "facility_id": st.facility_id,
                "stop_type": st.stop_type.value,
                "appt_start": st.appt_start,
                "appt_end": st.appt_end,
                "arrived_at": st.arrived_at,
                "departed_at": st.departed_at,
            }
            for s, _ in created
            for st in s.stops
        ],
    )
    conn.execute(
        insert(t.shipment_current_state),
        [{**_state_row(s), "version": s.version} for s, _ in created],
    )


_STATE_COLUMNS = (
    "status text, last_lat float8, last_lon float8, last_ping_at timestamptz, "
    "next_stop_sequence int2, eta timestamptz, eta_confidence float4, late_risk text, "
    "miles_remaining numeric, last_event_time timestamptz, status_event_time timestamptz, "
    "pre_exception_status text, carrier_event_time timestamptz, rate_event_time timestamptz, "
    "ewma_speed_mph float8, speed_var float8, speed_samples int4, hos_drive_hours float8, "
    "hos_rest_started_at timestamptz, hos_last_status text, late_risk_stop_sequence int2, "
    "suspect_lat float8, suspect_lon float8, suspect_at timestamptz"
)
_STATE_SET = ", ".join(f"{c.split()[0]} = v.{c.split()[0]}" for c in _STATE_COLUMNS.split(", "))


def _json_default(o: object) -> object:
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    raise TypeError(type(o))  # pragma: no cover


def update_states(conn: Connection, states: Sequence[ShipmentState]) -> None:
    """Bulk optimistic-concurrency update: ``WHERE version = expected``, ``version + 1``.

    Raises :class:`VersionConflictError` if any row was changed by another writer meanwhile.
    One statement for the whole batch: ``jsonb_to_recordset`` turns the rows into a relation.
    """
    if not states:
        return
    rows = [{**_state_row(s), "expected_version": s.version} for s in states]
    payload = json.dumps(rows, default=_json_default)
    updated = conn.execute(
        text(
            f"""
            UPDATE shipment_current_state AS c
            SET {_STATE_SET}, version = c.version + 1, updated_at = now()
            FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                 AS v(shipment_id text, expected_version int8, {_STATE_COLUMNS})
            WHERE c.shipment_id = v.shipment_id AND c.version = v.expected_version
            RETURNING c.shipment_id
            """
        ),
        {"rows": payload},
    ).scalars()
    got = set(updated)
    if len(got) != len(states):
        missing = sorted({s.shipment_id for s in states} - got)[:5]
        raise VersionConflictError(f"stale version for {missing}")
    # shipments: status / carrier / rate follow the state.
    conn.execute(
        text(
            """
            UPDATE shipments AS s
            SET status = v.status, carrier_id = v.carrier_id, rate_usd = v.rate_usd,
                updated_at = now()
            FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                 AS v(shipment_id text, status text, carrier_id text, rate_usd numeric)
            WHERE s.shipment_id = v.shipment_id
              AND (s.status, s.carrier_id, s.rate_usd)
                  IS DISTINCT FROM (v.status, v.carrier_id, v.rate_usd)
            """
        ),
        {
            "rows": json.dumps(
                [
                    {
                        "shipment_id": s.shipment_id,
                        "status": s.status.value,
                        "carrier_id": s.carrier_id,
                        "rate_usd": s.rate_usd,
                    }
                    for s in states
                ],
                default=_json_default,
            )
        },
    )


def update_stops(conn: Connection, states: Sequence[ShipmentState]) -> None:
    rows = [
        {
            "shipment_id": s.shipment_id,
            "stop_sequence": st.stop_sequence,
            "arrived_at": st.arrived_at,
            "departed_at": st.departed_at,
        }
        for s in states
        for st in s.stops
    ]
    if not rows:
        return
    conn.execute(
        text(
            """
            UPDATE shipment_stops AS st
            SET arrived_at = v.arrived_at, departed_at = v.departed_at
            FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                 AS v(shipment_id text, stop_sequence int2, arrived_at timestamptz,
                      departed_at timestamptz)
            WHERE st.shipment_id = v.shipment_id AND st.stop_sequence = v.stop_sequence
              AND (st.arrived_at, st.departed_at) IS DISTINCT FROM (v.arrived_at, v.departed_at)
            """
        ),
        {"rows": json.dumps(rows, default=_json_default)},
    )


def insert_lifecycle_events(conn: Connection, rows: Sequence[dict[str, Any]]) -> None:
    if rows:
        conn.execute(insert(t.shipment_events).on_conflict_do_nothing(), list(rows))


def insert_pings(conn: Connection, rows: Sequence[dict[str, Any]]) -> None:
    if rows:
        conn.execute(insert(t.gps_pings).on_conflict_do_nothing(), list(rows))


# --- parking (unknown shipment retry) ------------------------------------------------------------


def park_events(conn: Connection, rows: Sequence[dict[str, Any]]) -> None:
    if rows:
        conn.execute(insert(t.parked_events).on_conflict_do_nothing(), list(rows))


def take_resolvable_parked(conn: Connection, limit: int = 5000) -> list[tuple[str, str]]:
    """Remove and return (topic, raw) for parked events whose shipment now exists."""
    rows = conn.execute(
        text(
            """
            DELETE FROM parked_events p
            WHERE p.event_id IN (
                SELECT p2.event_id FROM parked_events p2
                JOIN shipments s USING (shipment_id)
                ORDER BY p2.first_seen_at
                LIMIT :limit
                FOR UPDATE OF p2 SKIP LOCKED
            )
            RETURNING p.topic, p.raw, p.first_seen_at
            """
        ),
        {"limit": limit},
    ).all()
    return [(r.topic, r.raw) for r in sorted(rows, key=lambda r: r.first_seen_at)]


def take_expired_parked(conn: Connection, retry_seconds: float) -> list[tuple[str, str]]:
    """Remove and return parked events whose shipment never appeared within the retry window."""
    rows = conn.execute(
        text(
            """
            DELETE FROM parked_events p
            WHERE p.event_id IN (
                SELECT p2.event_id FROM parked_events p2
                WHERE p2.first_seen_at < now() - make_interval(secs => :secs)
                  AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.shipment_id = p2.shipment_id)
                FOR UPDATE SKIP LOCKED
            )
            RETURNING p.topic, p.raw
            """
        ),
        {"secs": retry_seconds},
    ).all()
    return [(r.topic, r.raw) for r in rows]


def parked_count(conn: Connection) -> int:
    return int(conn.execute(text("SELECT count(*) FROM parked_events")).scalar_one())


# --- watermarks ---------------------------------------------------------------------------------


def advance_watermarks(
    conn: Connection, group: str, marks: dict[tuple[str, int], datetime]
) -> None:
    if not marks:
        return
    stmt = insert(t.stream_watermarks)
    stmt = stmt.on_conflict_do_update(
        index_elements=["consumer_group", "topic", "partition"],
        set_={
            "max_event_time": text(
                "GREATEST(stream_watermarks.max_event_time, excluded.max_event_time)"
            ),
            "updated_at": text("now()"),
        },
    )
    conn.execute(
        stmt,
        [
            {"consumer_group": group, "topic": tp, "partition": p, "max_event_time": ts}
            for (tp, p), ts in sorted(marks.items())
        ],
    )


def fleet_clock(conn: Connection) -> datetime | None:
    """The fleet's event-time clock: the newest event time any processor partition has seen."""
    value = conn.execute(text("SELECT max(max_event_time) FROM stream_watermarks")).scalar()
    return value if isinstance(value, datetime) else None

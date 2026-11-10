"""Read queries for the API (SQLAlchemy Core and raw SQL)."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import ColumnElement, Connection, RowMapping, and_, select, text

from freightpulse.storage import tables as t

# --- keyset cursor ---------------------------------------------------------------------------


class CursorError(ValueError):
    pass


def encode_cursor(after: str) -> str:
    return base64.urlsafe_b64encode(json.dumps({"after": after}).encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> str:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
        after = data["after"]
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise CursorError("malformed cursor") from exc
    if not isinstance(after, str):
        raise CursorError("malformed cursor")
    return after


def list_shipments(
    conn: Connection,
    *,
    status: str | None,
    carrier_id: str | None,
    late_risk: str | None,
    after: str | None,
    limit: int,
) -> list[RowMapping]:
    """Keyset pagination on ``shipment_id``: ``WHERE shipment_id > :after ORDER BY shipment_id``.

    Each filter is added only when given, so the planner can use
    ``ix_state_status_id`` / ``ix_state_at_risk`` / ``ix_shipments_carrier`` instead of a
    catch-all ``(:p IS NULL OR col = :p)`` predicate that defeats indexes.
    """
    c, s, lanes = t.shipment_current_state, t.shipments, t.lanes
    conds: list[ColumnElement[bool]] = []
    if status:
        conds.append(c.c.status == status)
    if late_risk:
        conds.append(c.c.late_risk == late_risk)
    if carrier_id:
        conds.append(s.c.carrier_id == carrier_id)
    if after:
        # The bound goes on *both* join inputs: Postgres propagates equalities across a join
        # key but not range predicates, so without the second one the merge join would scan
        # `shipments` from the first row (docs/sql_performance.md, case 3).
        conds.append(c.c.shipment_id > after)
        conds.append(s.c.shipment_id > after)
    stmt = (
        select(
            c.c.shipment_id,
            c.c.status,
            s.c.customer_id,
            s.c.carrier_id,
            s.c.lane_id,
            lanes.c.origin_facility_id,
            lanes.c.dest_facility_id,
            c.c.late_risk,
            c.c.eta,
            c.c.miles_remaining,
            c.c.last_ping_at,
            c.c.updated_at,
        )
        .select_from(
            c.join(s, s.c.shipment_id == c.c.shipment_id).join(
                lanes, lanes.c.lane_id == s.c.lane_id
            )
        )
        .where(and_(*conds) if conds else text("true"))
        .order_by(c.c.shipment_id)
        .limit(limit + 1)
    )
    return list(conn.execute(stmt).mappings())


def shipment_detail(conn: Connection, shipment_id: str) -> RowMapping | None:
    return (
        conn.execute(
            text(
                """
            SELECT c.*, s.mode, s.equipment, s.customer_id, cu.name AS customer_name,
                   s.carrier_id, ca.name AS carrier_name, s.rate_usd, s.lane_id,
                   l.distance_mi AS lane_distance_mi, s.created_at,
                   (SELECT count(*) FROM alerts a
                    WHERE a.shipment_id = c.shipment_id AND a.resolved_at IS NULL) AS active_alerts
            FROM shipment_current_state c
            JOIN shipments s USING (shipment_id)
            JOIN customers cu ON cu.customer_id = s.customer_id
            JOIN lanes l ON l.lane_id = s.lane_id
            LEFT JOIN carriers ca ON ca.carrier_id = s.carrier_id
            WHERE c.shipment_id = :sid
            """
            ),
            {"sid": shipment_id},
        )
        .mappings()
        .first()
    )


def shipment_exists(conn: Connection, shipment_id: str) -> bool:
    return (
        conn.execute(
            text("SELECT 1 FROM shipments WHERE shipment_id = :sid"), {"sid": shipment_id}
        ).first()
        is not None
    )


def shipment_stops(conn: Connection, shipment_id: str) -> Sequence[RowMapping]:
    return (
        conn.execute(
            text(
                """
            SELECT st.stop_sequence, st.facility_id, f.name AS facility_name, f.city, f.state,
                   f.lat, f.lon, st.stop_type, st.appt_start, st.appt_end, st.arrived_at,
                   st.departed_at
            FROM shipment_stops st JOIN facilities f USING (facility_id)
            WHERE st.shipment_id = :sid ORDER BY st.stop_sequence
            """
            ),
            {"sid": shipment_id},
        )
        .mappings()
        .all()
    )


def timeline(conn: Connection, shipment_id: str, limit: int) -> Sequence[RowMapping]:
    # Served by ix_events_shipment_time (shipment_id, event_time) on every partition.
    return (
        conn.execute(
            text(
                """
            SELECT event_id, event_type, event_time, ingested_at, source, payload
            FROM shipment_events WHERE shipment_id = :sid
            ORDER BY event_time, event_id LIMIT :limit
            """
            ),
            {"sid": shipment_id, "limit": limit},
        )
        .mappings()
        .all()
    )


def track(
    conn: Connection, shipment_id: str, since: datetime | None, limit: int
) -> Sequence[RowMapping]:
    params: dict[str, Any] = {"sid": shipment_id, "limit": limit}
    where = "shipment_id = :sid"
    if since is not None:
        where += " AND event_time >= :since"
        params["since"] = since
    return (
        conn.execute(
            text(
                f"SELECT event_time, lat, lon, speed_mph, driver_status FROM gps_pings "
                f"WHERE {where} ORDER BY event_time LIMIT :limit"
            ),
            params,
        )
        .mappings()
        .all()
    )


def alerts(
    conn: Connection,
    *,
    active: bool | None,
    alert_type: str | None,
    shipment_id: str | None,
    limit: int,
) -> Sequence[RowMapping]:
    a = t.alerts
    conds: list[ColumnElement[bool]] = []
    if active is True:
        conds.append(a.c.resolved_at.is_(None))
    elif active is False:
        conds.append(a.c.resolved_at.is_not(None))
    if alert_type:
        conds.append(a.c.alert_type == alert_type)
    if shipment_id:
        conds.append(a.c.shipment_id == shipment_id)
    stmt = (
        select(a)
        .where(and_(*conds) if conds else text("true"))
        .order_by(a.c.triggered_at.desc(), a.c.alert_id)
        .limit(limit)
    )
    return conn.execute(stmt).mappings().all()


def lane(conn: Connection, lane_id: int) -> RowMapping | None:
    return (
        conn.execute(text("SELECT * FROM lanes WHERE lane_id = :id"), {"id": lane_id})
        .mappings()
        .first()
    )


def lane_metrics(
    conn: Connection, lane_id: int, start: datetime | None, end: datetime | None
) -> Sequence[RowMapping]:
    params: dict[str, Any] = {"id": lane_id}
    where = "lane_id = :id"
    if start is not None:
        where += " AND hour_bucket >= :start"
        params["start"] = start
    if end is not None:
        where += " AND hour_bucket < :end"
        params["end"] = end
    return (
        conn.execute(
            text(f"SELECT * FROM lane_metrics_hourly WHERE {where} ORDER BY hour_bucket"), params
        )
        .mappings()
        .all()
    )


def carrier(conn: Connection, carrier_id: str) -> RowMapping | None:
    return (
        conn.execute(
            text("SELECT carrier_id, name, scac FROM carriers WHERE carrier_id = :id"),
            {"id": carrier_id},
        )
        .mappings()
        .first()
    )


def scorecard(conn: Connection, carrier_id: str, since_day: Any) -> Sequence[RowMapping]:
    return (
        conn.execute(
            text(
                "SELECT * FROM carrier_scorecard_daily WHERE carrier_id = :id AND day >= :since "
                "ORDER BY day"
            ),
            {"id": carrier_id, "since": since_day},
        )
        .mappings()
        .all()
    )


def fleet_summary(conn: Connection, now: datetime) -> dict[str, Any]:
    day_start = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo)
    by_status: dict[str, int] = dict(
        conn.execute(
            text("SELECT status, count(*) FROM shipment_current_state GROUP BY status")
        ).all()
    )
    at_risk = conn.execute(
        text(
            "SELECT count(*) FROM shipment_current_state "
            "WHERE late_risk <> 'LOW' AND status <> 'DELIVERED'"
        )
    ).scalar_one()
    active: dict[str, int] = dict(
        conn.execute(
            text("SELECT alert_type, count(*) FROM alerts WHERE resolved_at IS NULL GROUP BY 1")
        ).all()
    )
    detention = conn.execute(
        text(
            """
            SELECT count(*) FILTER (WHERE departed_at IS NULL
                                    AND :now - arrived_at > interval '2 hours') AS detained_now,
                   COALESCE(sum(GREATEST(0, extract(epoch FROM
                       (LEAST(COALESCE(departed_at, :now), :now) - arrived_at)) / 3600 - 2)
                   ) * 75, 0) AS cost
            FROM shipment_stops
            WHERE arrived_at IS NOT NULL AND arrived_at <= :now
              AND COALESCE(departed_at, :now) >= :day_start
            """
        ),
        {"now": now, "day_start": day_start},
    ).one()
    delivered_today = conn.execute(
        text(
            """
            SELECT count(*) FROM shipment_events
            WHERE event_type = 'shipment.delivered'
              AND event_time >= :day_start AND event_time < :day_end
            """
        ),
        {"day_start": day_start, "day_end": day_start + timedelta(days=1)},
    ).scalar_one()
    return {
        "shipments_total": sum(by_status.values()),
        "in_transit": by_status.get("IN_TRANSIT", 0),
        "at_risk": at_risk,
        "by_status": by_status,
        "active_alerts": active,
        "detained_now": detention.detained_now,
        "detention_cost_today_usd": round(float(detention.cost), 2),
        "delivered_today": delivered_today,
    }

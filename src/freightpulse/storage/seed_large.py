"""``make seed-large``: bulk-load a realistic large dataset with ``COPY`` for SQL performance work.

Defaults: 200,000 shipments (400k stops), 1,000,000 lifecycle events, 5,000,000 GPS pings and
300,000 alerts spread over the last 30 days. Rows are written in (near) event-time order, the way
a stream would append them, which is what makes BRIN effective. The target database is created
if missing, migrated, partitioned and seeded with reference data, then ANALYZEd and rolled up.

Usage::

    python -m freightpulse.storage.seed_large --database-url postgresql+psycopg://.../freightpulse_perf
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from sqlalchemy import make_url, text

from freightpulse.config import get_settings
from freightpulse.domain.geo import interpolate, road_miles
from freightpulse.domain.reference_data import CARRIERS, CUSTOMERS
from freightpulse.processing.rollups import run_rollups
from freightpulse.simulator.fleet import candidate_lanes
from freightpulse.storage.db import make_engine, run_migrations
from freightpulse.storage.partitions import ensure_partitions
from freightpulse.storage.seed import seed_reference_data

log = logging.getLogger("seed-large")

LIFECYCLE = (
    ("shipment.created", 0),
    ("shipment.tendered", 5 * 60),
    ("shipment.tender_accepted", 25 * 60),
    ("stop.arrived", 2 * 3600),
    ("stop.departed", 3 * 3600 + 1800),
)


def _plain_dsn(url: str) -> str:
    u = make_url(url)
    return u.set(drivername="postgresql").render_as_string(hide_password=False)


def ensure_database(url: str) -> None:
    u = make_url(url)
    admin = _plain_dsn(url).rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (u.database,)
        ).fetchone()
        if not exists:
            conn.execute(f'CREATE DATABASE "{u.database}"')
            log.info("created database %s", u.database)


def _uuid(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def seed(
    url: str,
    *,
    shipments: int = 200_000,
    events: int = 1_000_000,
    pings: int = 5_000_000,
    alerts: int = 300_000,
    days: int = 30,
    seed_value: int = 7,
) -> dict[str, Any]:
    rng = random.Random(seed_value)
    ensure_database(url)
    run_migrations(url)
    engine = make_engine(url, pool_size=1)
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    with engine.begin() as conn:
        ensure_partitions(conn, [start.date() + timedelta(days=i) for i in range(-1, days + 9)])
    seed_reference_data(engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE alerts, shipment_current_state, shipment_stops, shipments, lanes, "
                "shipment_events, gps_pings, lane_metrics_hourly, carrier_scorecard_daily, "
                "parked_events, stream_watermarks RESTART IDENTITY CASCADE"
            )
        )

    lanes = candidate_lanes()[:400]
    carriers = [c.carrier_id for c in CARRIERS]
    customers = [c.customer_id for c in CUSTOMERS]
    span = (now - start).total_seconds()
    timings: dict[str, float] = {}

    with psycopg.connect(_plain_dsn(url)) as conn:
        # Lanes
        t0 = time.perf_counter()
        with conn.cursor().copy(
            "COPY lanes (origin_facility_id, dest_facility_id, distance_mi) FROM STDIN"
        ) as cp:
            for o, d in lanes:
                cp.write_row(
                    (o.facility_id, d.facility_id, round(road_miles(o.location, d.location), 1))
                )

        # Shipments, stops, current state. Shipment i is created at start + i/N of the span.
        ship_meta: list[tuple[str, int, str, datetime]] = []
        statuses = (
            ["DELIVERED"] * 70
            + ["IN_TRANSIT"] * 12
            + ["AT_PICKUP"] * 5
            + ["AT_DELIVERY"] * 3
            + ["ACCEPTED"] * 4
            + ["TENDERED"] * 3
            + ["CREATED"] * 3
        )
        with conn.cursor().copy(
            "COPY shipments (shipment_id, customer_id, carrier_id, lane_id, mode, equipment, status, "
            "rate_usd, created_at, updated_at) FROM STDIN"
        ) as cp:
            for i in range(shipments):
                sid = f"SHP-{i + 1:07d}"
                created = start + timedelta(seconds=span * i / shipments)
                lane_id = rng.randrange(1, len(lanes) + 1)
                carrier = rng.choice(carriers)
                status = rng.choice(statuses)
                ship_meta.append((sid, lane_id, carrier, created))
                cp.write_row(
                    (
                        sid,
                        rng.choice(customers),
                        carrier,
                        lane_id,
                        "FTL" if rng.random() < 0.85 else "LTL",
                        rng.choice(["dry_van", "dry_van", "reefer", "flatbed"]),
                        status,
                        round(rng.uniform(600, 4500), 2),
                        created,
                        created + timedelta(hours=20),
                    )
                )
        conn.commit()
        with conn.cursor().copy(
            "COPY shipment_stops (shipment_id, stop_sequence, facility_id, stop_type, appt_start, "
            "appt_end, arrived_at, departed_at) FROM STDIN"
        ) as cp:
            for sid, lane_id, _carrier, created in ship_meta:
                o, d = lanes[lane_id - 1]
                pu = created + timedelta(hours=2)
                transit = timedelta(hours=rng.uniform(5, 30))
                do = pu + transit
                arr1 = pu + timedelta(minutes=rng.gauss(0, 25))
                dep1 = arr1 + timedelta(minutes=rng.lognormvariate(4.4, 0.45))
                arr2 = do + timedelta(minutes=rng.gauss(0, 60))
                dep2 = arr2 + timedelta(minutes=rng.lognormvariate(4.4, 0.45))
                done = arr2 < now
                cp.write_row(
                    (
                        sid,
                        1,
                        o.facility_id,
                        "PICKUP",
                        pu,
                        pu + timedelta(hours=2),
                        arr1 if arr1 < now else None,
                        dep1 if dep1 < now else None,
                    )
                )
                cp.write_row(
                    (
                        sid,
                        2,
                        d.facility_id,
                        "DELIVERY",
                        do,
                        do + timedelta(hours=2),
                        arr2 if done else None,
                        dep2 if dep2 < now else None,
                    )
                )
        with conn.cursor().copy(
            "COPY shipment_current_state (shipment_id, status, last_lat, last_lon, last_ping_at, "
            "next_stop_sequence, eta, eta_confidence, late_risk, miles_remaining, last_event_time, "
            "version, status_event_time) FROM STDIN"
        ) as cp:
            for sid, lane_id, _carrier, created in ship_meta:
                o, d = lanes[lane_id - 1]
                p = interpolate(o.location, d.location, rng.random())
                last = min(now, created + timedelta(hours=rng.uniform(3, 30)))
                risk = rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[85, 8, 7])[0]
                cp.write_row(
                    (
                        sid,
                        "IN_TRANSIT",
                        p.lat,
                        p.lon,
                        last,
                        2,
                        last + timedelta(hours=6),
                        0.8,
                        risk,
                        250.0,
                        last,
                        3,
                        last,
                    )
                )
        conn.execute(
            "UPDATE shipment_current_state c SET status = s.status FROM shipments s "
            "WHERE s.shipment_id = c.shipment_id"
        )
        conn.commit()
        timings["shipments_stops_state_s"] = time.perf_counter() - t0

        # Lifecycle events: 5 per shipment + tender rejections to reach the target count.
        t0 = time.perf_counter()
        extra = max(0, events - shipments * len(LIFECYCLE))
        written = 0
        chunk = 5000
        with conn.cursor().copy(
            "COPY shipment_events (event_id, shipment_id, event_type, event_time, ingested_at, "
            "source, payload) FROM STDIN"
        ) as cp:
            for c0 in range(0, shipments, chunk):
                rows = []
                for sid, _lane, carrier, created in ship_meta[c0 : c0 + chunk]:
                    for et, offset_s in LIFECYCLE:
                        t = created + timedelta(seconds=offset_s + rng.uniform(0, 120))
                        payload: dict[str, Any] = {}
                        if et == "shipment.tendered":
                            payload = {"carrier_id": carrier, "rate_usd": "2450.00"}
                        elif et == "shipment.tender_accepted":
                            payload = {"carrier_id": carrier}
                        elif et.startswith("stop."):
                            payload = {"stop_sequence": 1, "facility_id": "FAC-DAL-01"}
                        else:
                            payload = {"customer_id": "CUS-001", "mode": "FTL"}
                        rows.append((t, sid, et, payload))
                    if extra and rng.random() < extra / shipments:
                        t = created + timedelta(minutes=rng.uniform(1, 4))
                        rows.append(
                            (
                                t,
                                sid,
                                "shipment.tender_rejected",
                                {"carrier_id": rng.choice(carriers), "reason": "no capacity"},
                            )
                        )
                rows.sort(key=lambda r: r[0])
                for t, sid, et, payload in rows:
                    if written >= events:
                        break
                    cp.write_row(
                        (
                            _uuid(rng),
                            sid,
                            et,
                            t,
                            t + timedelta(seconds=1),
                            "seed-large",
                            json.dumps(payload),
                        )
                    )
                    written += 1
        conn.commit()
        timings["events_s"] = time.perf_counter() - t0
        events_written = written

        # GPS pings in time order; each ping belongs to a shipment active around that time.
        t0 = time.perf_counter()
        per_day = pings // days
        written = 0
        for day in range(days):
            n = per_day if day < days - 1 else pings - written
            day_start = start + timedelta(days=day)
            offsets = sorted(rng.uniform(0, 86400) for _ in range(n))
            # One COPY per day, committed between days (never commit inside an open COPY).
            with conn.cursor().copy(
                "COPY gps_pings (event_id, shipment_id, event_time, lat, lon, speed_mph, "
                "driver_status, ingested_at) FROM STDIN"
            ) as cp:
                for off in offsets:
                    t = day_start + timedelta(seconds=off)
                    frac = (t - start).total_seconds() / span
                    idx = min(shipments - 1, max(0, int(frac * shipments) - rng.randrange(0, 300)))
                    sid, lane_id, _c, _created = ship_meta[idx]
                    o, _d = lanes[lane_id - 1]
                    moving = rng.random() < 0.8
                    cp.write_row(
                        (
                            _uuid(rng),
                            sid,
                            t,
                            o.lat + rng.uniform(-3, 3),
                            o.lon + rng.uniform(-3, 3),
                            round(rng.uniform(45, 68), 1) if moving else 0.0,
                            "DRIVING" if moving else rng.choice(["ON_DUTY", "OFF_DUTY", "SLEEPER"]),
                            t + timedelta(seconds=rng.uniform(0.1, 2)),
                        )
                    )
                    written += 1
            conn.commit()
            log.info("gps_pings: day %d/%d (%d rows)", day + 1, days, written)
        timings["gps_pings_s"] = time.perf_counter() - t0
        pings_written = written

        # Alerts: mostly resolved history, ~3% still active.
        t0 = time.perf_counter()
        with conn.cursor().copy(
            "COPY alerts (alert_id, shipment_id, alert_type, severity, message, triggered_at, "
            "resolved_at, dedupe_key, details) FROM STDIN"
        ) as cp:
            for i in range(alerts):
                sid, _l, _c, created = ship_meta[rng.randrange(shipments)]
                kind = rng.choice(["LATE_RISK", "DETENTION", "STALE_TRACKING"])
                trig = created + timedelta(hours=rng.uniform(1, 20))
                resolved = (
                    None if rng.random() < 0.03 else trig + timedelta(hours=rng.uniform(0.5, 6))
                )
                cp.write_row(
                    (
                        _uuid(rng),
                        sid,
                        kind,
                        rng.choice(["MEDIUM", "HIGH", "CRITICAL"]),
                        f"{kind.lower()} on {sid}",
                        trig,
                        resolved,
                        f"seed:{i}",
                        "{}",
                    )
                )
        conn.commit()
        timings["alerts_s"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    with engine.begin() as c:
        c.execute(text("ANALYZE"))
    with engine.begin() as c:
        run_rollups(c, now, window=timedelta(days=days + 1))
    with engine.begin() as c:
        c.execute(text("ANALYZE"))
    timings["analyze_rollups_s"] = time.perf_counter() - t0
    with engine.connect() as c:
        counts = {
            t: c.execute(text(f"SELECT count(*) FROM {t}")).scalar_one()
            for t in (
                "shipments",
                "shipment_stops",
                "shipment_events",
                "gps_pings",
                "alerts",
                "carrier_scorecard_daily",
                "lane_metrics_hourly",
            )
        }
    engine.dispose()
    return {
        "counts": counts,
        "events_written": events_written,
        "pings_written": pings_written,
        "timings_s": {k: round(v, 1) for k, v in timings.items()},
        "window": f"{start.isoformat()} .. {now.isoformat()}",
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description="Bulk-load a large dataset with COPY")
    p.add_argument("--database-url", default=get_settings().database_url)
    p.add_argument("--shipments", type=int, default=200_000)
    p.add_argument("--events", type=int, default=1_000_000)
    p.add_argument("--pings", type=int, default=5_000_000)
    p.add_argument("--alerts", type=int, default=300_000)
    p.add_argument("--days", type=int, default=30)
    a = p.parse_args()
    started = time.perf_counter()
    result = seed(
        a.database_url,
        shipments=a.shipments,
        events=a.events,
        pings=a.pings,
        alerts=a.alerts,
        days=a.days,
    )
    result["total_s"] = round(time.perf_counter() - started, 1)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()

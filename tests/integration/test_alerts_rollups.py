from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import Engine, text

from freightpulse.config import Settings
from freightpulse.domain.alerts import AlertMessage
from freightpulse.domain.enums import AlertAction, AlertType, Severity
from freightpulse.domain.events import EventEnvelope
from freightpulse.domain.geo import interpolate
from freightpulse.processing.alert_sink import AlertSink, apply_alerts
from freightpulse.processing.jobs import Jobs
from freightpulse.processing.processor import Inbound, StateProcessor
from freightpulse.processing.rollups import run_rollups
from freightpulse.processing.scanners import scan_detention, scan_stale_tracking
from tests.factories import DALLAS, MEMPHIS, T0, EventFactory
from tests.integration.helpers import count, produce, read_topic, run_processor

pytestmark = pytest.mark.integration


def feed(proc: StateProcessor, events: list[EventEnvelope]) -> None:
    """Push events through the processor's batch path directly (no consumer group)."""
    items = [
        Inbound(
            raw=e.to_json_bytes(),
            topic=proc.settings.telemetry_topic
            if e.event_type.is_telemetry
            else proc.settings.shipment_events_topic,
            key=e.shipment_id,
            partition=0,
            offset=i,
        )
        for i, e in enumerate(events)
    ]
    proc.process(items, sweep=True)


def _alerts(engine: Engine) -> list[dict[str, object]]:
    with engine.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text(
                    "SELECT alert_type, severity, dedupe_key, resolved_at, details "
                    "FROM alerts ORDER BY triggered_at, dedupe_key"
                )
            ).mappings()
        ]


def test_detention_scanner_escalates_and_resolves(settings: Settings, engine: Engine) -> None:
    proc = StateProcessor(settings, engine=engine)
    f = EventFactory()
    events = f.full_lifecycle(pings=1)[:4]  # created, tendered, accepted, arrived at pickup
    arrived = events[3].event_time
    feed(proc, events)
    with engine.connect() as conn:
        assert scan_detention(conn, arrived + timedelta(hours=1, minutes=59)) == []
        first = scan_detention(conn, arrived + timedelta(hours=3, minutes=30))
    assert [(a.alert_type, a.severity) for a in first] == [(AlertType.DETENTION, Severity.HIGH)]
    assert first[0].details["detention_cost_usd"] == 112.5
    with engine.begin() as conn:
        apply_alerts(conn, first)
        apply_alerts(conn, first)  # replay is a no-op
    with engine.connect() as conn:
        assert scan_detention(conn, arrived + timedelta(hours=3, minutes=40)) == []  # no change
        crit = scan_detention(conn, arrived + timedelta(hours=4, minutes=30))
    assert [a.severity for a in crit] == [Severity.CRITICAL]
    with engine.begin() as conn:
        apply_alerts(conn, crit)
    rows = _alerts(engine)
    assert len(rows) == 1
    assert rows[0]["severity"] == "CRITICAL"

    feed(proc, [f.departed(arrived + timedelta(hours=5), 1, DALLAS[0])])
    with engine.connect() as conn:
        resolved = scan_detention(conn, arrived + timedelta(hours=5, minutes=5))
    assert [a.action for a in resolved] == [AlertAction.RESOLVE]
    with engine.begin() as conn:
        apply_alerts(conn, resolved)
    rows = _alerts(engine)
    assert rows[0]["resolved_at"] is not None
    assert rows[0]["details"]["detention_cost_usd"] == 225.0  # type: ignore[index]
    proc.close()


def test_stale_tracking_scanner(settings: Settings, engine: Engine) -> None:
    proc = StateProcessor(settings, engine=engine)
    f = EventFactory()
    events = f.full_lifecycle(pings=3)[:7]  # ... departed pickup, 2 pings
    last_ping = events[-1].event_time
    feed(proc, events)
    with engine.connect() as conn:
        assert scan_stale_tracking(conn, last_ping + timedelta(minutes=59)) == []
        alerts = scan_stale_tracking(conn, last_ping + timedelta(minutes=95))
    assert [(a.alert_type, a.severity) for a in alerts] == [
        (AlertType.STALE_TRACKING, Severity.MEDIUM)
    ]
    with engine.begin() as conn:
        apply_alerts(conn, alerts)
    feed(
        proc, [f.ping(last_ping + timedelta(minutes=100), interpolate(DALLAS[1], MEMPHIS[1], 0.5))]
    )
    with engine.connect() as conn:
        resolved = scan_stale_tracking(conn, last_ping + timedelta(minutes=101))
    assert [a.action for a in resolved] == [AlertAction.RESOLVE]
    with engine.begin() as conn:
        apply_alerts(conn, resolved)
    assert _alerts(engine)[0]["resolved_at"] is not None
    proc.close()


def test_sink_supersedes_late_risk_and_replays_idempotently(
    engine: Engine, db_settings: Settings
) -> None:
    from freightpulse.domain.alerts import late_risk_alert
    from freightpulse.domain.enums import LateRisk
    from tests.factories import uid

    f = EventFactory()
    # The sink needs the shipment row (FK); create it via SQL quickly.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO lanes (origin_facility_id, dest_facility_id, distance_mi) "
                "VALUES ('FAC-DAL-01', 'FAC-MEM-01', 495)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO shipments (shipment_id, customer_id, lane_id, mode, equipment, "
                "status, created_at, updated_at) VALUES (:s, 'CUS-001', 1, 'FTL', 'dry_van', "
                "'IN_TRANSIT', now(), now())"
            ),
            {"s": f.shipment_id},
        )

    def lr(prev: LateRisk, level: LateRisk, minutes: int) -> AlertMessage:
        a = late_risk_alert(
            shipment_id=f.shipment_id,
            stop_sequence=2,
            previous=prev,
            level=level,
            eta=T0,
            appt_end=T0,
            trigger_event_id=uid(),
            at=T0 + timedelta(minutes=minutes),
        )
        assert a is not None
        return a

    seq = [
        lr(LateRisk.LOW, LateRisk.MEDIUM, 1),
        lr(LateRisk.MEDIUM, LateRisk.HIGH, 2),
        lr(LateRisk.HIGH, LateRisk.LOW, 3),
        lr(LateRisk.LOW, LateRisk.HIGH, 4),
    ]
    with engine.begin() as conn:
        apply_alerts(conn, seq)
    first = _alerts(engine)
    with engine.begin() as conn:
        apply_alerts(conn, seq)  # full replay
    assert _alerts(engine) == first
    active = [a for a in first if a["resolved_at"] is None]
    assert len(first) == 3
    assert [(a["severity"]) for a in active] == ["HIGH"]


def test_rollups_lane_carrier_and_dwell(settings: Settings, engine: Engine) -> None:
    proc = StateProcessor(settings, engine=engine)
    for i in range(3):
        f = EventFactory(t0=T0 + timedelta(minutes=10 * i))
        feed(proc, f.full_lifecycle(pings=3))
    rejected = EventFactory(t0=T0)
    feed(
        proc,
        [
            rejected.created(T0),
            rejected.tendered(T0 + timedelta(minutes=1), carrier="CAR-001"),
            rejected.rejected(T0 + timedelta(minutes=2), carrier="CAR-001"),
        ],
    )
    with engine.begin() as conn:
        counts = run_rollups(conn, T0 + timedelta(hours=12))
        again = run_rollups(conn, T0 + timedelta(hours=12))
    assert counts == again
    assert counts["lane_metrics_hourly"] >= 1
    with engine.connect() as conn:
        lane = conn.execute(
            text(
                "SELECT sum(shipments_delivered), min(on_time_pct), max(avg_transit_hours), "
                "max(p90_transit_hours) FROM lane_metrics_hourly"
            )
        ).one()
        card = conn.execute(
            text(
                "SELECT tenders, acceptance_rate, on_time_pickup_pct, on_time_delivery_pct, "
                "tracking_compliance_pct FROM carrier_scorecard_daily "
                "WHERE carrier_id = 'CAR-001' AND day = :d"
            ),
            {"d": date(2026, 11, 10)},
        ).one()
        dwell = conn.execute(
            text("SELECT avg_dwell_min FROM facilities WHERE facility_id = 'FAC-DAL-01'")
        ).scalar_one()
    assert lane[0] == 3
    assert float(lane[1]) == 100.0
    assert 6 < float(lane[2]) < 10  # ~8.3h Dallas -> Memphis at 60 mph + 0.2h
    assert card.tenders == 4
    assert float(card.acceptance_rate) == 75.0
    assert float(card.on_time_pickup_pct) == 100.0
    assert float(card.on_time_delivery_pct) == 100.0
    assert float(card.tracking_compliance_pct) == 100.0
    assert float(dwell) == 60.0
    proc.close()


def test_end_to_end_alerts_through_kafka(settings: Settings, engine: Engine) -> None:
    """processor -> freight.alerts -> alert sink -> alerts table; jobs.scan -> same path."""
    f = EventFactory()
    events = [
        f.created(T0, delivery_window=(T0 + timedelta(hours=3), T0 + timedelta(hours=4))),
        f.tendered(T0 + timedelta(minutes=1)),
        f.accepted(T0 + timedelta(minutes=2)),
        f.arrived(T0 + timedelta(minutes=5), 1, DALLAS[0]),
        f.departed(T0 + timedelta(minutes=10), 1, DALLAS[0]),
        *[
            f.ping(T0 + timedelta(minutes=15 + i), interpolate(DALLAS[1], MEMPHIS[1], 0.001 * i))
            for i in range(5)
        ],
    ]
    produce(settings, events)
    run_processor(settings, engine)
    jobs = Jobs(settings, engine=engine)
    assert jobs.scan(T0 + timedelta(hours=2)) == 1  # stale tracking (last ping ~T0+19m)
    jobs.rollup(T0 + timedelta(hours=2))
    msgs = read_topic(settings, settings.alerts_topic, expect=2)
    assert len(msgs) == 2
    sink = AlertSink(settings, engine=engine)
    sink.run(max_seconds=20, idle_exit_s=2)
    sink.close()
    kinds = sorted((a["alert_type"], a["severity"]) for a in _alerts(engine))
    assert kinds == [("LATE_RISK", "HIGH"), ("STALE_TRACKING", "MEDIUM")]
    assert count(engine, "SELECT count(*) FROM alerts WHERE resolved_at IS NULL") == 2
    jobs.partitions()

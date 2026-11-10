"""End-to-end processor tests: produce to Redpanda -> processor -> assert Postgres state."""

from __future__ import annotations

import json
import random
import signal
import time
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import Engine, text

from freightpulse.config import Settings
from freightpulse.domain.alerts import AlertMessage
from freightpulse.domain.enums import AlertType, DlqErrorType, LateRisk, ShipmentStatus
from freightpulse.domain.events import EventEnvelope
from freightpulse.domain.geo import Point, interpolate
from freightpulse.processing.processor import StateProcessor
from freightpulse.storage import repositories as repo
from tests.factories import DALLAS, MEMPHIS, T0, EventFactory
from tests.integration.helpers import (
    count,
    dlq_records,
    produce,
    read_topic,
    run_processor,
    spawn_processor,
)

pytestmark = pytest.mark.integration


def _state(engine: Engine, sid: str) -> dict[str, object]:
    with engine.connect() as conn:
        row = (
            conn.execute(
                text(
                    "SELECT c.status, c.last_lat, c.last_lon, c.last_ping_at, "
                    "c.next_stop_sequence, c.last_event_time, s.carrier_id, s.rate_usd, "
                    "s.status AS ship_status "
                    "FROM shipment_current_state c JOIN shipments s USING (shipment_id) "
                    "WHERE shipment_id = :sid"
                ),
                {"sid": sid},
            )
            .mappings()
            .one()
        )
        stops = conn.execute(
            text(
                "SELECT stop_sequence, arrived_at, departed_at FROM shipment_stops "
                "WHERE shipment_id = :sid ORDER BY 1"
            ),
            {"sid": sid},
        ).all()
    return {**row, "stops": [tuple(s) for s in stops]}


def test_full_lifecycle_lands_in_postgres(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    events = f.full_lifecycle(pings=8, with_exception=True)
    produce(settings, events)
    proc = run_processor(settings, engine)
    st = _state(engine, f.shipment_id)
    assert st["status"] == st["ship_status"] == ShipmentStatus.DELIVERED
    assert st["carrier_id"] == "CAR-001"
    assert all(a and d for _, a, d in st["stops"])
    lifecycle = sum(1 for e in events if not e.event_type.is_telemetry)
    assert count(engine, "SELECT count(*) FROM shipment_events") == lifecycle
    assert count(engine, "SELECT count(*) FROM gps_pings") == 8
    assert count(engine, "SELECT count(*) FROM lanes") == 1
    assert proc.totals.committed_lifecycle + proc.totals.committed_pings == len(events)
    # Watermark = newest event time seen.
    with engine.connect() as conn:
        assert repo.fleet_clock(conn) == max(e.event_time for e in events)


def test_duplicates_produce_exactly_one_row(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    events = f.full_lifecycle(pings=5)
    # Every event three times: twice back to back and once more after everything else.
    doubled = [e for e in events for _ in range(2)] + events
    produce(settings, doubled)
    proc = run_processor(settings, engine)
    assert count(engine, "SELECT count(*) FROM gps_pings") == 5
    assert count(engine, "SELECT count(*) FROM shipment_events") == len(events) - 5
    assert count(engine, "SELECT count(DISTINCT event_id) FROM shipment_events") == len(events) - 5
    assert proc.totals.duplicates == 2 * len(events)
    assert _state(engine, f.shipment_id)["status"] == ShipmentStatus.DELIVERED


def test_shuffled_order_converges_to_ordered_state(settings: Settings, engine: Engine) -> None:
    ordered_f = EventFactory()
    shuffled_f = EventFactory(t0=ordered_f.t0)
    ordered = ordered_f.full_lifecycle(pings=6, with_exception=True)
    # Same lifecycle for a second shipment id, shuffled (created still first: else it parks).
    shuffled = [
        e.model_copy(update={"shipment_id": shuffled_f.shipment_id, "event_id": uuid.uuid4()})
        for e in ordered
    ]
    rest = shuffled[1:]
    random.Random(7).shuffle(rest)
    produce(settings, ordered)
    produce(settings, [shuffled[0], *rest])
    run_processor(settings, engine)
    a = _state(engine, ordered_f.shipment_id)
    b = _state(engine, shuffled_f.shipment_id)
    assert a == b
    assert a["status"] == ShipmentStatus.DELIVERED
    assert count(
        engine,
        "SELECT count(*) FROM shipment_events WHERE shipment_id = :s",
        s=shuffled_f.shipment_id,
    ) == count(
        engine,
        "SELECT count(*) FROM shipment_events WHERE shipment_id = :s",
        s=ordered_f.shipment_id,
    )


def test_malformed_and_invalid_events_land_in_dlq(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    events = f.full_lifecycle(pings=3)
    good_ping = events[5]
    bad_json = (settings.telemetry_topic, f.shipment_id, b'{"event_id": "nope", ')
    bad_payload = json.loads(good_ping.to_json_bytes())
    bad_payload["payload"]["lat"] = 999
    jump = f.ping(events[-4].event_time + timedelta(seconds=30), Point(47.6, -122.3))
    invalid = f.departed(events[-1].event_time + timedelta(hours=1), 1, DALLAS[0])
    bad_carrier = f.accepted(events[2].event_time + timedelta(seconds=1), carrier="CAR-999")
    produce(settings, events)
    produce(
        settings,
        [
            bad_json,
            (settings.telemetry_topic, f.shipment_id, json.dumps(bad_payload).encode()),
            jump,
            invalid,
            bad_carrier,
        ],
    )
    run_processor(settings, engine)
    recs = dlq_records(settings, expect=5)
    kinds = sorted(r.error_type for r in recs)
    assert kinds == sorted(
        [
            DlqErrorType.SCHEMA_VALIDATION,
            DlqErrorType.SCHEMA_VALIDATION,
            DlqErrorType.IMPOSSIBLE_GPS_JUMP,
            DlqErrorType.INVALID_TRANSITION,
            DlqErrorType.UNKNOWN_REFERENCE,
        ]
    )
    for r in recs:
        assert r.original_topic.startswith(settings.topic_prefix)
        assert r.error_detail
        assert r.failed_at
    assert any(r.original_bytes() == b'{"event_id": "nope", ' for r in recs)
    # Nothing invalid reached the log.
    assert count(engine, "SELECT count(*) FROM gps_pings") == 3
    assert _state(engine, f.shipment_id)["status"] == ShipmentStatus.DELIVERED


def test_unknown_shipment_is_parked_then_applied(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    events = f.full_lifecycle(pings=4)
    # GPS + lifecycle arrive before shipment.created (which lags).
    produce(settings, events[1:])
    proc = StateProcessor(settings, engine=engine)
    proc.run(max_seconds=4, idle_exit_s=1.5)
    assert proc.totals.parked == len(events) - 1
    assert count(engine, "SELECT count(*) FROM parked_events") == len(events) - 1
    produce(settings, [events[0]])
    proc.run(max_seconds=10, idle_exit_s=2)
    proc.close()
    assert count(engine, "SELECT count(*) FROM parked_events") == 0
    assert _state(engine, f.shipment_id)["status"] == ShipmentStatus.DELIVERED
    assert count(engine, "SELECT count(*) FROM gps_pings") == 4


def test_unknown_shipment_goes_to_dlq_after_retry(settings: Settings, engine: Engine) -> None:
    orphan = EventFactory()
    produce(settings, [orphan.ping(T0, DALLAS[1])])
    proc = StateProcessor(settings, engine=engine)
    proc.run(max_seconds=8, idle_exit_s=5)  # retry window in tests is 2s
    proc.close()
    recs = dlq_records(settings, expect=1)
    assert [r.error_type for r in recs] == [DlqErrorType.UNKNOWN_SHIPMENT]
    assert count(engine, "SELECT count(*) FROM parked_events") == 0


def test_late_risk_alert_published_once(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    events = [
        f.created(T0, delivery_window=(T0 + timedelta(hours=3), T0 + timedelta(hours=4))),
        f.tendered(T0 + timedelta(minutes=1)),
        f.accepted(T0 + timedelta(minutes=2)),
        f.arrived(T0 + timedelta(minutes=5), 1, DALLAS[0]),
        f.departed(T0 + timedelta(minutes=10), 1, DALLAS[0]),
    ] + [
        f.ping(T0 + timedelta(minutes=15 + i), interpolate(DALLAS[1], MEMPHIS[1], 0.001 * i))
        for i in range(10)
    ]
    produce(settings, events)
    run_processor(settings, engine)
    with engine.connect() as conn:
        risk = conn.execute(
            text("SELECT late_risk FROM shipment_current_state WHERE shipment_id=:s"),
            {"s": f.shipment_id},
        ).scalar_one()
    assert risk == LateRisk.HIGH
    alerts = [
        AlertMessage.model_validate_json(v)
        for v in read_topic(settings, settings.alerts_topic, expect=1)
    ]
    assert len(alerts) == 1
    assert alerts[0].alert_type is AlertType.LATE_RISK
    assert alerts[0].produced_at is not None


def test_version_conflict_is_detected(settings: Settings, engine: Engine) -> None:
    f = EventFactory()
    produce(settings, f.full_lifecycle(pings=1)[:3])
    run_processor(settings, engine)
    with engine.connect() as conn:
        state = repo.load_states(
            conn, [f.shipment_id], StateProcessor(settings, engine=engine).reference.facilities
        )[f.shipment_id]
    with engine.begin() as conn:
        conn.execute(text("UPDATE shipment_current_state SET version = version + 1"))
    with pytest.raises(repo.VersionConflictError), engine.begin() as conn:
        repo.update_states(conn, [state])


def _many_shipments(n: int) -> tuple[list[EventEnvelope], int, int]:
    events: list[EventEnvelope] = []
    for i in range(n):
        f = EventFactory(t0=T0 + timedelta(minutes=i))
        events += f.full_lifecycle(pings=6)
    pings = sum(1 for e in events if e.event_type.is_telemetry)
    return events, len(events) - pings, pings


@pytest.mark.parametrize("fault", ["crash_before_db_commit:3", "crash_after_db_commit:3"])
def test_crash_mid_batch_loses_nothing_and_duplicates_nothing(
    settings: Settings, engine: Engine, fault: str
) -> None:
    settings = settings.model_copy(update={"processor_batch_size": 25})
    events, lifecycle, pings = _many_shipments(15)
    produce(settings, events)

    crashed = spawn_processor(settings, FREIGHTPULSE_FAULT_INJECTION=fault)
    crashed_out, _ = crashed.communicate(timeout=120)
    assert crashed.returncode == 137, crashed_out.decode()[-2000:]
    partial = count(engine, "SELECT count(*) FROM gps_pings")
    assert partial < pings  # it really died part-way

    restarted = spawn_processor(settings)
    out, _ = restarted.communicate(timeout=120)
    assert restarted.returncode == 0, out.decode()[-2000:]

    assert count(engine, "SELECT count(*) FROM gps_pings") == pings
    assert count(engine, "SELECT count(DISTINCT event_id) FROM gps_pings") == pings
    assert count(engine, "SELECT count(*) FROM shipment_events") == lifecycle
    assert (
        count(engine, "SELECT count(*) FROM shipment_current_state WHERE status='DELIVERED'") == 15
    )
    if fault.startswith("crash_after"):
        # The crashed batch was redelivered and recognised as already processed.
        assert b"dup=" in out and b"dup=0 " not in out


def test_sigkill_at_random_point_then_restart(settings: Settings, engine: Engine) -> None:
    settings = settings.model_copy(update={"processor_batch_size": 20})
    events, lifecycle, pings = _many_shipments(20)
    produce(settings, events)
    proc = spawn_processor(settings)
    deadline = time.monotonic() + 60
    while count(engine, "SELECT count(*) FROM gps_pings") < 20 and time.monotonic() < deadline:
        time.sleep(0.05)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=30)
    restarted = spawn_processor(settings)
    out, _ = restarted.communicate(timeout=120)
    assert restarted.returncode == 0, out.decode()[-2000:]
    assert count(engine, "SELECT count(*) FROM gps_pings") == pings
    assert count(engine, "SELECT count(*) FROM shipment_events") == lifecycle
    assert (
        count(engine, "SELECT count(*) FROM shipment_current_state WHERE status='DELIVERED'") == 20
    )


def test_dlq_replay_after_fix(settings: Settings, engine: Engine) -> None:
    from freightpulse.processing.dlq_replay import replay

    f = EventFactory()
    events = f.full_lifecycle(pings=2)
    # The carrier is not in the reference data yet: tender events dead-letter.
    events = [
        f.tendered(e.event_time, carrier="CAR-777")
        if e.event_type == "shipment.tendered"
        else f.accepted(e.event_time, carrier="CAR-777")
        if e.event_type == "shipment.tender_accepted"
        else e
        for e in events
    ]
    produce(settings, events)
    run_processor(settings, engine)
    recs = dlq_records(settings, expect=2)
    assert {r.error_type for r in recs} == {DlqErrorType.UNKNOWN_REFERENCE}
    assert _state(engine, f.shipment_id)["carrier_id"] is None

    group = f"replay-{uuid.uuid4().hex[:6]}"
    assert replay(settings, group_id=group, dry_run=True)["would_replay:UNKNOWN_REFERENCE"] == 2
    # The fix: onboard the carrier.
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO carriers (carrier_id, name, scac, mc_number) "
                "VALUES ('CAR-777', 'Late Onboard Freight', 'LOBF', 'MC-1')"
            )
        )
    stats = replay(settings, group_id=group)
    assert stats["replayed:UNKNOWN_REFERENCE"] == 2
    assert replay(settings, group_id=group) == {}  # nothing left to replay
    run_processor(settings, engine)
    st = _state(engine, f.shipment_id)
    assert st["carrier_id"] == "CAR-777"
    assert str(st["rate_usd"]) == "2450.00"
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE shipments CASCADE"))
        conn.execute(text("DELETE FROM carriers WHERE carrier_id = 'CAR-777'"))


def test_three_processors_share_the_load_without_deadlocks(
    settings: Settings, engine: Engine
) -> None:
    """Regression: concurrent lane upserts used to deadlock and kill instances (found by bench)."""
    from freightpulse.domain.reference_data import FACILITIES

    settings = settings.model_copy(update={"processor_batch_size": 200})
    events: list[EventEnvelope] = []
    lanes = [(a, b) for a in FACILITIES[:6] for b in FACILITIES[6:12]]
    for i in range(240):
        a, b = lanes[i % len(lanes)]
        f = EventFactory(t0=T0 + timedelta(seconds=i))
        events += [
            f.created(
                f.t0, pickup=(a.facility_id, a.location), delivery=(b.facility_id, b.location)
            ),
            f.tendered(f.t0 + timedelta(minutes=1)),
            f.ping(f.t0 + timedelta(minutes=2), a.location),
        ]
    procs = [spawn_processor(settings, idle_exit=10) for _ in range(3)]
    time.sleep(8)  # let the group form before producing
    produce(settings, events)
    outputs = [p.communicate(timeout=180)[0].decode() for p in procs]
    for p, out in zip(procs, outputs, strict=True):
        assert p.returncode == 0, out[-3000:]
        assert "Traceback" not in out, out[-3000:]
    # Work was actually spread across instances.
    busy = [o for o in outputs if "consumed=0 " not in o]
    assert len(busy) >= 2, outputs
    assert count(engine, "SELECT count(*) FROM shipments") == 240
    assert count(engine, "SELECT count(*) FROM shipment_events") == 480
    assert count(engine, "SELECT count(*) FROM gps_pings") == 240
    assert count(engine, "SELECT count(*) FROM lanes") == len(lanes)

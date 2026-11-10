from __future__ import annotations

import ast
import json
import pathlib
from datetime import timedelta
from uuid import uuid4

import pytest

from freightpulse.domain.alerts import (
    alert_id_for,
    detention_alert,
    detention_cost_usd,
    detention_resolved,
    detention_severity,
    is_stale,
    late_risk_alert,
    stale_tracking_alert,
    stale_tracking_resolved,
)
from freightpulse.domain.enums import AlertAction, AlertType, EventType, LateRisk, Severity
from freightpulse.domain.events import EventValidationError, parse_event
from tests.factories import DALLAS, T0, EventFactory


def _raw(env_bytes: bytes, **changes: object) -> bytes:
    data = json.loads(env_bytes)
    for k, v in changes.items():
        if v is _DELETE:
            data.pop(k)
        else:
            data[k] = v
    return json.dumps(data).encode()


_DELETE = object()


# --- event contracts ------------------------------------------------------------------------


def test_round_trip_every_event_type() -> None:
    f = EventFactory()
    envs = [*f.full_lifecycle(pings=2, with_exception=True), f.rejected(T0 + timedelta(minutes=10))]
    seen = set()
    for env in envs:
        parsed = parse_event(env.to_json_bytes())
        assert parsed.envelope == env
        assert parsed.event_id == env.event_id
        assert parsed.shipment_id == f.shipment_id
        seen.add(parsed.event_type)
    assert seen == set(EventType)


@pytest.mark.parametrize(
    ("mutate", "fragment"),
    [
        (lambda b: b"not json at all", "envelope"),
        (lambda b: _raw(b, event_id="nope"), "event_id"),
        (lambda b: _raw(b, event_type="shipment.teleported"), "event_type"),
        (lambda b: _raw(b, shipment_id="ABC-1"), "shipment_id"),
        (lambda b: _raw(b, schema_version=2), "schema_version"),
        (lambda b: _raw(b, event_time="2026-11-10T08:00:00"), "event_time"),  # naive
        (lambda b: _raw(b, event_time="1999-01-01T00:00:00Z"), "implausibly far"),
        (lambda b: _raw(b, source=_DELETE), "source"),
        (lambda b: _raw(b, extra_field=1), "extra_field"),
    ],
)
def test_envelope_validation_errors(mutate: object, fragment: str) -> None:
    env = EventFactory().ping(T0, DALLAS[1])
    with pytest.raises(EventValidationError) as exc:
        parse_event(mutate(env.to_json_bytes()))  # type: ignore[operator]
    assert fragment in exc.value.detail


@pytest.mark.parametrize(
    ("payload_change", "fragment"),
    [
        ({"lat": 123.0}, "lat"),
        ({"speed_mph": -1}, "speed_mph"),
        ({"driver_status": "NAPPING"}, "driver_status"),
        ({"heading": 360}, "heading"),
    ],
)
def test_gps_payload_validation(payload_change: dict[str, object], fragment: str) -> None:
    env = EventFactory().ping(T0, DALLAS[1])
    data = json.loads(env.to_json_bytes())
    data["payload"].update(payload_change)
    with pytest.raises(EventValidationError) as exc:
        parse_event(json.dumps(data))
    assert fragment in exc.value.detail
    assert exc.value.detail.startswith("payload")


def test_created_payload_rules() -> None:
    env = EventFactory().created()
    data = json.loads(env.to_json_bytes())
    stops = data["payload"]["stops"]
    for bad, fragment in [
        ([stops[0]], "at least 2"),
        ([stops[1], stops[0]], "1..n"),
        ([dict(stops[0], stop_type="DELIVERY"), stops[1]], "first stop"),
        ([stops[0], dict(stops[1], stop_type="PICKUP")], "last stop"),
        ([dict(stops[0], appt_end="2026-11-10T00:00:00Z"), stops[1]], "appt_end"),
    ]:
        d = json.loads(json.dumps(data))
        d["payload"]["stops"] = bad
        with pytest.raises(EventValidationError) as exc:
            parse_event(json.dumps(d))
        assert fragment in exc.value.detail


def test_event_time_normalised_to_utc() -> None:
    env = EventFactory().ping(T0, DALLAS[1])
    data = json.loads(env.to_json_bytes())
    data["event_time"] = "2026-11-10T03:00:00-05:00"
    parsed = parse_event(json.dumps(data))
    assert parsed.event_time == T0
    assert parsed.event_time.utcoffset() == timedelta(0)


# --- alerts ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("hours", "severity", "cost"),
    [
        (1.9, None, 0.0),
        (2.0, Severity.MEDIUM, 0.0),
        (2.5, Severity.MEDIUM, 37.5),
        (3.0, Severity.HIGH, 75.0),
        (4.0, Severity.CRITICAL, 150.0),
        (6.0, Severity.CRITICAL, 300.0),
    ],
)
def test_detention_severity_and_cost(hours: float, severity: Severity | None, cost: float) -> None:
    dwell = timedelta(hours=hours)
    assert detention_severity(dwell) is severity
    assert detention_cost_usd(dwell) == cost


def test_detention_alert_is_deterministic_and_escalates_under_one_key() -> None:
    args = {
        "shipment_id": "SHP-000001",
        "stop_sequence": 2,
        "facility_id": "FAC-X",
        "arrived_at": T0,
    }
    assert detention_alert(**args, now=T0 + timedelta(hours=1)) is None  # type: ignore[arg-type]
    a = detention_alert(**args, now=T0 + timedelta(hours=2, minutes=30))  # type: ignore[arg-type]
    b = detention_alert(**args, now=T0 + timedelta(hours=4, minutes=30))  # type: ignore[arg-type]
    assert a is not None and b is not None
    assert a.dedupe_key == b.dedupe_key == "detention:SHP-000001:2"
    assert a.alert_id == b.alert_id == alert_id_for(a.dedupe_key)
    assert (a.severity, b.severity) == (Severity.MEDIUM, Severity.CRITICAL)
    assert b.details["detention_cost_usd"] == 187.5
    r = detention_resolved(
        shipment_id="SHP-000001", stop_sequence=2, departed_at=T0, dwell=timedelta(hours=3)
    )
    assert r.action is AlertAction.RESOLVE and r.dedupe_key == a.dedupe_key


def test_late_risk_alert_only_on_level_change() -> None:
    eid = uuid4()
    common = {
        "shipment_id": "SHP-000001",
        "stop_sequence": 2,
        "eta": T0 + timedelta(hours=5),
        "appt_end": T0 + timedelta(hours=4),
        "trigger_event_id": eid,
        "at": T0,
    }
    assert late_risk_alert(previous=LateRisk.HIGH, level=LateRisk.HIGH, **common) is None  # type: ignore[arg-type]
    high = late_risk_alert(previous=LateRisk.LOW, level=LateRisk.HIGH, **common)  # type: ignore[arg-type]
    assert high is not None and high.severity is Severity.HIGH
    assert "60 min after" in high.message
    assert str(eid) in high.dedupe_key
    low = late_risk_alert(previous=LateRisk.HIGH, level=LateRisk.LOW, **common)  # type: ignore[arg-type]
    assert low is not None and low.action is AlertAction.RESOLVE


def test_stale_tracking() -> None:
    assert not is_stale(None, T0)
    assert not is_stale(T0, T0 + timedelta(minutes=60))
    assert is_stale(T0, T0 + timedelta(minutes=61))
    assert (
        stale_tracking_alert(
            shipment_id="SHP-000001", last_ping_at=T0, now=T0 + timedelta(minutes=30)
        )
        is None
    )
    a = stale_tracking_alert(
        shipment_id="SHP-000001", last_ping_at=T0, now=T0 + timedelta(minutes=90)
    )
    b = stale_tracking_alert(shipment_id="SHP-000001", last_ping_at=T0, now=T0 + timedelta(hours=4))
    assert a is not None and b is not None
    assert a.alert_type is AlertType.STALE_TRACKING
    assert (a.severity, b.severity) == (Severity.MEDIUM, Severity.HIGH)
    assert a.dedupe_key == b.dedupe_key
    r = stale_tracking_resolved(shipment_id="SHP-000001", last_ping_at=T0, at=T0, reason="ping")
    assert r.dedupe_key == a.dedupe_key and r.action is AlertAction.RESOLVE
    assert json.loads(a.to_json_bytes())["alert_type"] == "STALE_TRACKING"


# --- purity ---------------------------------------------------------------------------------

FORBIDDEN = {"confluent_kafka", "sqlalchemy", "psycopg", "httpx", "requests", "socket", "fastapi"}


def test_domain_layer_has_no_io_imports() -> None:
    domain = pathlib.Path(__file__).parents[2] / "src" / "freightpulse" / "domain"
    for path in domain.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for name in names:
                assert name.split(".")[0] not in FORBIDDEN, f"{path.name} imports {name}"

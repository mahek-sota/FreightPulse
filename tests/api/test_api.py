"""API tests against a database seeded through the real processor path."""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from freightpulse.api.app import create_app
from freightpulse.config import Settings
from freightpulse.domain.alerts import detention_alert, stale_tracking_alert
from freightpulse.domain.events import EventEnvelope
from freightpulse.processing.alert_sink import apply_alerts
from freightpulse.processing.processor import Inbound, StateProcessor
from freightpulse.processing.rollups import run_rollups
from freightpulse.processing.topics import ensure_topics
from freightpulse.storage.db import make_engine
from tests.conftest import DYNAMIC_TABLES
from tests.factories import DALLAS, T0, EventFactory

pytestmark = pytest.mark.integration

N = 40


@pytest.fixture(scope="module")
def seeded(pg_url: str, kafka_bootstrap: str) -> Iterator[tuple[Settings, Engine, dict[str, Any]]]:
    tag = uuid.uuid4().hex[:8]
    settings = Settings(  # type: ignore[call-arg]
        _env_file=None,
        database_url=pg_url,
        kafka_bootstrap_servers=kafka_bootstrap,
        topic_prefix=f"api{tag}.",
    )
    ensure_topics(settings)
    engine = make_engine(pg_url, pool_size=5)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(DYNAMIC_TABLES)} RESTART IDENTITY CASCADE"))
    proc = StateProcessor(settings, engine=engine)
    info: dict[str, Any] = {"ids": [], "status": {}, "carrier": {}}
    for i in range(N):
        carrier = f"CAR-00{1 + i % 3}"
        f = EventFactory(t0=T0 + timedelta(minutes=i))
        evs = f.full_lifecycle(pings=4)
        evs = [
            f.tendered(e.event_time, carrier=carrier)
            if e.event_type == "shipment.tendered"
            else f.accepted(e.event_time, carrier=carrier)
            if e.event_type == "shipment.tender_accepted"
            else e
            for e in evs
        ]
        cut = {0: len(evs), 1: 7, 2: 4, 3: 2}[
            i % 4
        ]  # delivered / in transit / at pickup / tendered
        evs = evs[:cut]
        _feed(proc, evs)
        info["ids"].append(f.shipment_id)
        info["carrier"][f.shipment_id] = carrier
        info["status"][f.shipment_id] = {
            0: "DELIVERED",
            1: "IN_TRANSIT",
            2: "AT_PICKUP",
            3: "TENDERED",
        }[i % 4]
        info.setdefault("events", {})[f.shipment_id] = evs
    proc.close()
    # Alerts: detention on every AT_PICKUP shipment, stale tracking on the first in-transit one.
    alerts = []
    with engine.connect() as conn:
        for sid, arrived in conn.execute(
            text(
                "SELECT shipment_id, arrived_at FROM shipment_stops "
                "WHERE arrived_at IS NOT NULL AND departed_at IS NULL"
            )
        ).all():
            a = detention_alert(
                shipment_id=sid,
                stop_sequence=1,
                facility_id=DALLAS[0],
                arrived_at=arrived,
                now=arrived + timedelta(hours=3),
            )
            assert a is not None
            alerts.append(a)
        sid, last = conn.execute(
            text(
                "SELECT shipment_id, last_ping_at FROM shipment_current_state "
                "WHERE status='IN_TRANSIT' ORDER BY 1 LIMIT 1"
            )
        ).one()
        stale = stale_tracking_alert(
            shipment_id=sid, last_ping_at=last, now=last + timedelta(hours=2)
        )
        assert stale is not None
        alerts.append(stale)
    with engine.begin() as conn:
        apply_alerts(conn, alerts)
        run_rollups(conn, T0 + timedelta(hours=14))
    info["detention_ids"] = sorted(a.shipment_id for a in alerts if a.alert_type == "DETENTION")
    info["stale_id"] = sid
    yield settings, engine, info
    engine.dispose()
    from freightpulse.processing.topics import delete_topics

    delete_topics(settings)


def _feed(proc: StateProcessor, events: list[EventEnvelope]) -> None:
    items = [
        Inbound(
            raw=e.to_json_bytes(),
            topic=proc.settings.shipment_events_topic,
            key=e.shipment_id,
            partition=0,
            offset=i,
        )
        for i, e in enumerate(events)
    ]
    proc.process(items, sweep=False)


@pytest.fixture(scope="module")
def client(seeded: tuple[Settings, Engine, dict[str, Any]]) -> Iterator[TestClient]:
    settings, engine, _ = seeded
    with TestClient(create_app(settings, engine, start_broadcaster=False)) as c:
        yield c


@pytest.fixture(scope="module")
def info(seeded: tuple[Settings, Engine, dict[str, Any]]) -> dict[str, Any]:
    return seeded[2]


def _err(resp: Any, status: int, code: str) -> None:
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert body["error"]["message"]


# --- health -------------------------------------------------------------------------------------


def test_health_and_ready(
    client: TestClient, seeded: tuple[Settings, Engine, dict[str, Any]]
) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    r = client.get("/ready")
    assert r.status_code == 200
    assert r.json() == {"status": "ready", "postgres": True, "broker": True}
    settings, engine, _ = seeded
    broken = settings.model_copy(update={"kafka_bootstrap_servers": "127.0.0.1:1"})
    with TestClient(create_app(broken, engine, start_broadcaster=False)) as c:
        r = c.get("/ready")
    assert r.status_code == 503
    assert r.json()["broker"] is False


# --- shipments ----------------------------------------------------------------------------------


def test_keyset_pagination_walks_every_shipment_once(
    client: TestClient, info: dict[str, Any]
) -> None:
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, Any] = {"limit": 7}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/shipments", params=params).json()
        seen += [s["shipment_id"] for s in body["items"]]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert seen == sorted(info["ids"])
    assert pages == -(-N // 7)


def test_shipment_filters(client: TestClient, info: dict[str, Any]) -> None:
    body = client.get("/shipments", params={"status": "IN_TRANSIT", "limit": 500}).json()
    expected = sorted(s for s, st in info["status"].items() if st == "IN_TRANSIT")
    assert [s["shipment_id"] for s in body["items"]] == expected
    body = client.get("/shipments", params={"carrier_id": "CAR-002", "limit": 500}).json()
    expected_c = sorted(
        s for s, c in info["carrier"].items() if c == "CAR-002" and info["status"][s] != "TENDERED"
    )
    assert [s["shipment_id"] for s in body["items"]] == expected_c
    body = client.get("/shipments", params={"late_risk": "LOW", "status": "DELIVERED"}).json()
    assert all(s["status"] == "DELIVERED" for s in body["items"])


def test_shipment_list_errors(client: TestClient) -> None:
    _err(client.get("/shipments", params={"cursor": "%%%not-base64"}), 400, "invalid_cursor")
    _err(client.get("/shipments", params={"limit": 100000}), 422, "validation_error")
    _err(client.get("/shipments", params={"status": "LOST_AT_SEA"}), 422, "validation_error")


def test_shipment_detail(client: TestClient, info: dict[str, Any]) -> None:
    sid = next(s for s, st in info["status"].items() if st == "IN_TRANSIT")
    d = client.get(f"/shipments/{sid}").json()
    assert d["status"] == "IN_TRANSIT"
    assert d["carrier_id"] == info["carrier"][sid]
    assert d["carrier_name"]
    assert d["customer_name"]
    assert [s["stop_sequence"] for s in d["stops"]] == [1, 2]
    assert d["stops"][0]["facility_name"]
    assert d["stops"][0]["dwell_minutes"] == 60.0
    assert d["eta"]["eta"] is not None
    assert d["eta"]["next_stop_sequence"] == 2
    assert 0 < d["eta"]["effective_confidence"] <= d["eta"]["eta_confidence"] <= 1
    assert d["version"] >= 1
    _err(client.get("/shipments/SHP-999999"), 404, "shipment_not_found")


def test_timeline_is_ordered_by_event_time(client: TestClient, info: dict[str, Any]) -> None:
    sid = next(s for s, st in info["status"].items() if st == "DELIVERED")
    body = client.get(f"/shipments/{sid}/timeline").json()
    times = [e["event_time"] for e in body["events"]]
    assert times == sorted(times)
    assert body["events"][0]["event_type"] == "shipment.created"
    assert body["events"][-1]["event_type"] == "shipment.delivered"
    assert all(e["event_type"] != "gps.ping" for e in body["events"])
    _err(client.get("/shipments/SHP-999999/timeline"), 404, "shipment_not_found")


def test_track_with_since(client: TestClient, info: dict[str, Any]) -> None:
    sid = next(s for s, st in info["status"].items() if st == "DELIVERED")
    pts = client.get(f"/shipments/{sid}/track").json()["points"]
    assert len(pts) == 4
    later = client.get(f"/shipments/{sid}/track", params={"since": pts[2]["event_time"]}).json()
    assert later["points"] == pts[2:]
    _err(client.get("/shipments/SHP-999999/track"), 404, "shipment_not_found")


# --- alerts -------------------------------------------------------------------------------------


def test_alerts_filters(client: TestClient, info: dict[str, Any]) -> None:
    all_active = client.get("/alerts", params={"active": "true"}).json()["items"]
    assert len(all_active) == len(info["detention_ids"]) + 1
    det = client.get("/alerts", params={"active": "true", "type": "DETENTION"}).json()["items"]
    assert sorted(a["shipment_id"] for a in det) == info["detention_ids"]
    assert all(a["details"]["detention_cost_usd"] == 75.0 for a in det)
    stale = client.get("/alerts", params={"type": "STALE_TRACKING"}).json()["items"]
    assert [a["shipment_id"] for a in stale] == [info["stale_id"]]
    assert client.get("/alerts", params={"active": "false"}).json()["items"] == []
    _err(client.get("/alerts", params={"type": "ALIENS"}), 422, "validation_error")


# --- analytics ----------------------------------------------------------------------------------


def test_lane_metrics(client: TestClient) -> None:
    body = client.get("/lanes/1/metrics").json()
    assert body["origin_facility_id"] == DALLAS[0]
    assert sum(r["shipments_delivered"] for r in body["rows"]) == N // 4
    first_hour = body["rows"][0]["hour_bucket"]
    narrowed = client.get("/lanes/1/metrics", params={"from": first_hour, "to": first_hour}).json()
    assert narrowed["rows"] == []
    _err(client.get("/lanes/999/metrics"), 404, "lane_not_found")


def test_carrier_scorecard(client: TestClient) -> None:
    body = client.get("/carriers/CAR-001/scorecard", params={"days": 30}).json()
    assert body["scac"] == "LSFW"
    assert body["tenders"] > 0
    assert body["acceptance_rate"] == 100.0
    assert body["daily"]
    _err(client.get("/carriers/CAR-404/scorecard"), 404, "carrier_not_found")
    _err(client.get("/carriers/CAR-001/scorecard", params={"days": 0}), 422, "validation_error")


def test_metrics_summary(client: TestClient, info: dict[str, Any]) -> None:
    s = client.get("/metrics/summary").json()
    assert s["shipments_total"] == N
    assert s["in_transit"] == sum(1 for v in info["status"].values() if v == "IN_TRANSIT")
    assert s["by_status"]["DELIVERED"] == N // 4
    assert s["active_alerts"]["DETENTION"] == len(info["detention_ids"])
    assert s["detained_now"] >= 0
    assert s["detention_cost_today_usd"] >= 0
    assert s["delivered_today"] == N // 4


def test_openapi_prometheus_and_index(client: TestClient) -> None:
    spec = client.get("/openapi.json").json()
    for path in [
        "/shipments",
        "/shipments/{shipment_id}",
        "/shipments/{shipment_id}/timeline",
        "/shipments/{shipment_id}/track",
        "/alerts",
        "/lanes/{lane_id}/metrics",
        "/carriers/{carrier_id}/scorecard",
        "/metrics/summary",
        "/health",
        "/ready",
    ]:
        assert path in spec["paths"], path
    assert client.get("/docs").status_code == 200
    assert (
        "http_requests" in client.get("/metrics").text
        or "python_info" in client.get("/metrics").text
    )
    assert "/ws/alerts" in client.get("/").text
    _err(client.get("/nope"), 404, "not_found")


# --- websocket ----------------------------------------------------------------------------------


def test_websocket_pushes_live_alerts(seeded: tuple[Settings, Engine, dict[str, Any]]) -> None:
    settings, engine, info = seeded
    app = create_app(settings, engine, start_broadcaster=True)
    alert = stale_tracking_alert(
        shipment_id=info["stale_id"], last_ping_at=T0, now=T0 + timedelta(hours=5)
    )
    assert alert is not None
    with TestClient(app) as c, c.websocket_connect("/ws/alerts") as ws:
        assert ws.receive_json()["type"] == "hello"
        assert app.state.broadcaster.ready.wait(30)
        time.sleep(1.0)  # let the consumer settle at the latest offset
        from confluent_kafka import Producer

        p = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        p.produce(settings.alerts_topic, key=alert.shipment_id, value=alert.to_json_bytes())
        p.flush(10)
        got = json.loads(ws.receive_text())
        assert got["dedupe_key"] == alert.dedupe_key
        assert got["alert_type"] == "STALE_TRACKING"

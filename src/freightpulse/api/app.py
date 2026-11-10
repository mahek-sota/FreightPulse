"""FreightPulse API: REST over the relational model plus a live alert WebSocket."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import Connection, Engine, text
from starlette.exceptions import HTTPException as StarletteHTTPException

from freightpulse.api import queries as q
from freightpulse.api.page import INDEX_HTML
from freightpulse.api.schemas import (
    AlertList,
    AlertOut,
    ErrorResponse,
    EtaOut,
    FleetSummary,
    Health,
    LaneMetrics,
    LaneMetricsRow,
    Readiness,
    Scorecard,
    ScorecardDay,
    ShipmentDetail,
    ShipmentPage,
    ShipmentSummary,
    StopOut,
    Timeline,
    TimelineEvent,
    Track,
    TrackPoint,
)
from freightpulse.api.ws import AlertBroadcaster
from freightpulse.config import Settings, get_settings
from freightpulse.domain.enums import AlertType, LateRisk, ShipmentStatus
from freightpulse.domain.eta import degrade_confidence
from freightpulse.processing.topics import broker_reachable
from freightpulse.storage.db import make_engine
from freightpulse.storage.repositories import fleet_clock


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def _error(status: int, code: str, message: str, details: Any = None) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "details": details}}
    return JSONResponse(status_code=status, content=body)


def get_conn(request: Request) -> Iterator[Connection]:
    """One pooled connection per request (sync endpoints run in FastAPI's threadpool)."""
    engine: Engine = request.app.state.engine
    with engine.connect() as conn:
        yield conn


Db = Annotated[Connection, Depends(get_conn)]

ERRORS: dict[int | str, dict[str, Any]] = {
    404: {"model": ErrorResponse, "description": "Not found"},
    400: {"model": ErrorResponse, "description": "Bad request"},
    422: {"model": ErrorResponse, "description": "Validation error"},
}


def create_app(
    settings: Settings | None = None,
    engine: Engine | None = None,
    *,
    start_broadcaster: bool = True,
) -> FastAPI:
    settings = settings or get_settings()
    engine = engine or make_engine(settings.database_url, pool_size=10)
    broadcaster = AlertBroadcaster(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if start_broadcaster:
            broadcaster.start(asyncio.get_running_loop())
        yield
        broadcaster.stop()

    app = FastAPI(
        title="FreightPulse API",
        version="0.1.0",
        description=(
            "Near real-time freight visibility: shipment state, ETAs, late-risk, detention and "
            "stale-tracking alerts, lane and carrier analytics."
        ),
        lifespan=lifespan,
    )
    app.state.broadcaster = broadcaster
    app.state.engine = engine
    app.state.settings = settings

    # -- errors -----------------------------------------------------------------------------

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _error(exc.status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> JSONResponse:
        details = [{"loc": list(e.get("loc", ())), "msg": e.get("msg", "")} for e in exc.errors()]
        return _error(422, "validation_error", "request validation failed", details)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "not_found" if exc.status_code == 404 else f"http_{exc.status_code}"
        return _error(exc.status_code, code, str(exc.detail))

    # -- dependencies -----------------------------------------------------------------------

    def now_for(conn: Connection) -> datetime:
        return fleet_clock(conn) or datetime.now(UTC)

    def require_shipment(conn: Connection, shipment_id: str) -> None:
        if not q.shipment_exists(conn, shipment_id):
            raise ApiError(404, "shipment_not_found", f"shipment {shipment_id} not found")

    # -- health -----------------------------------------------------------------------------

    @app.get("/health", response_model=Health, tags=["health"])
    def health() -> Health:
        return Health()

    @app.get(
        "/ready",
        response_model=Readiness,
        tags=["health"],
        responses={503: {"model": Readiness, "description": "Not ready"}},
    )
    def ready() -> Response:
        pg = True
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception:
            pg = False
        broker = broker_reachable(settings)
        ok = pg and broker
        body = Readiness(status="ready" if ok else "not_ready", postgres=pg, broker=broker)
        return JSONResponse(status_code=200 if ok else 503, content=body.model_dump())

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # -- shipments --------------------------------------------------------------------------

    @app.get("/shipments", response_model=ShipmentPage, responses=ERRORS, tags=["shipments"])
    def list_shipments(
        conn: Db,
        status: ShipmentStatus | None = None,
        carrier_id: str | None = None,
        late_risk: LateRisk | None = None,
        cursor: str | None = Query(None, description="Keyset cursor from a previous page"),
        limit: int = Query(settings.api_default_page_size, ge=1, le=settings.api_max_page_size),
    ) -> ShipmentPage:
        after = None
        if cursor:
            try:
                after = q.decode_cursor(cursor)
            except q.CursorError as exc:
                raise ApiError(400, "invalid_cursor", str(exc)) from exc
        rows = q.list_shipments(
            conn,
            status=status.value if status else None,
            carrier_id=carrier_id,
            late_risk=late_risk.value if late_risk else None,
            after=after,
            limit=limit,
        )
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [ShipmentSummary.model_validate(dict(r)) for r in rows]
        next_cursor = q.encode_cursor(rows[-1]["shipment_id"]) if has_more and rows else None
        return ShipmentPage(items=items, next_cursor=next_cursor, limit=limit)

    @app.get(
        "/shipments/{shipment_id}",
        response_model=ShipmentDetail,
        responses=ERRORS,
        tags=["shipments"],
    )
    def get_shipment(shipment_id: str, conn: Db) -> ShipmentDetail:
        row = q.shipment_detail(conn, shipment_id)
        if row is None:
            raise ApiError(404, "shipment_not_found", f"shipment {shipment_id} not found")
        now = now_for(conn)
        stops = []
        for s in q.shipment_stops(conn, shipment_id):
            dwell = None
            if s["arrived_at"] is not None:
                end = s["departed_at"] or max(now, s["arrived_at"])
                dwell = round((end - s["arrived_at"]).total_seconds() / 60, 1)
            stops.append(StopOut(**dict(s), dwell_minutes=dwell))
        effective = None
        if row["eta_confidence"] is not None and row["last_ping_at"] is not None:
            age_min = max(0.0, (now - row["last_ping_at"]).total_seconds() / 60)
            effective = degrade_confidence(float(row["eta_confidence"]), age_min)
        return ShipmentDetail(
            shipment_id=row["shipment_id"],
            status=row["status"],
            pre_exception_status=row["pre_exception_status"],
            mode=row["mode"],
            equipment=row["equipment"],
            customer_id=row["customer_id"],
            customer_name=row["customer_name"],
            carrier_id=row["carrier_id"],
            carrier_name=row["carrier_name"],
            rate_usd=row["rate_usd"],
            lane_id=row["lane_id"],
            lane_distance_mi=float(row["lane_distance_mi"]),
            last_lat=row["last_lat"],
            last_lon=row["last_lon"],
            last_ping_at=row["last_ping_at"],
            last_event_time=row["last_event_time"],
            eta=EtaOut(
                eta=row["eta"],
                eta_confidence=row["eta_confidence"],
                effective_confidence=effective,
                miles_remaining=(
                    float(row["miles_remaining"]) if row["miles_remaining"] is not None else None
                ),
                next_stop_sequence=row["next_stop_sequence"],
                late_risk=row["late_risk"],
            ),
            stops=stops,
            active_alerts=row["active_alerts"],
            version=row["version"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @app.get(
        "/shipments/{shipment_id}/timeline",
        response_model=Timeline,
        responses=ERRORS,
        tags=["shipments"],
    )
    def get_timeline(
        shipment_id: str, conn: Db, limit: int = Query(500, ge=1, le=5000)
    ) -> Timeline:
        require_shipment(conn, shipment_id)
        events = [
            TimelineEvent.model_validate(dict(r)) for r in q.timeline(conn, shipment_id, limit)
        ]
        return Timeline(shipment_id=shipment_id, events=events)

    @app.get(
        "/shipments/{shipment_id}/track",
        response_model=Track,
        responses=ERRORS,
        tags=["shipments"],
    )
    def get_track(
        shipment_id: str,
        conn: Db,
        since: datetime | None = Query(None, description="Only pings at or after this time"),
        limit: int = Query(5000, ge=1, le=50_000),
    ) -> Track:
        require_shipment(conn, shipment_id)
        points = [
            TrackPoint.model_validate(dict(r)) for r in q.track(conn, shipment_id, since, limit)
        ]
        return Track(shipment_id=shipment_id, points=points)

    # -- alerts -----------------------------------------------------------------------------

    @app.get("/alerts", response_model=AlertList, responses=ERRORS, tags=["alerts"])
    def list_alerts(
        conn: Db,
        active: bool | None = Query(None, description="true = unresolved, false = resolved"),
        type: AlertType | None = Query(None, description="LATE_RISK | DETENTION | STALE_TRACKING"),
        shipment_id: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ) -> AlertList:
        rows = q.alerts(
            conn,
            active=active,
            alert_type=type.value if type else None,
            shipment_id=shipment_id,
            limit=limit,
        )
        return AlertList(items=[AlertOut.model_validate(dict(r)) for r in rows])

    # -- analytics --------------------------------------------------------------------------

    @app.get(
        "/lanes/{lane_id}/metrics", response_model=LaneMetrics, responses=ERRORS, tags=["analytics"]
    )
    def get_lane_metrics(
        lane_id: int,
        conn: Db,
        from_: datetime | None = Query(None, alias="from"),
        to: datetime | None = None,
    ) -> LaneMetrics:
        lane = q.lane(conn, lane_id)
        if lane is None:
            raise ApiError(404, "lane_not_found", f"lane {lane_id} not found")
        rows = q.lane_metrics(conn, lane_id, from_, to)
        return LaneMetrics(
            lane_id=lane_id,
            origin_facility_id=lane["origin_facility_id"],
            dest_facility_id=lane["dest_facility_id"],
            distance_mi=float(lane["distance_mi"]),
            rows=[LaneMetricsRow.model_validate(dict(r)) for r in rows],
        )

    @app.get(
        "/carriers/{carrier_id}/scorecard",
        response_model=Scorecard,
        responses=ERRORS,
        tags=["analytics"],
    )
    def get_scorecard(carrier_id: str, conn: Db, days: int = Query(30, ge=1, le=365)) -> Scorecard:
        car = q.carrier(conn, carrier_id)
        if car is None:
            raise ApiError(404, "carrier_not_found", f"carrier {carrier_id} not found")
        since = (now_for(conn) - timedelta(days=days - 1)).date()
        rows = q.scorecard(conn, carrier_id, since)
        daily = [ScorecardDay.model_validate(dict(r)) for r in rows]
        tenders = sum(d.tenders for d in daily)

        def weighted(attr: str, weight: str = "tenders") -> float | None:
            pairs = [
                (float(getattr(d, attr)), max(1, getattr(d, weight)))
                for d in daily
                if getattr(d, attr) is not None
            ]
            if not pairs:
                return None
            return round(float(sum(v * w for v, w in pairs) / sum(w for _, w in pairs)), 2)

        return Scorecard(
            carrier_id=car["carrier_id"],
            name=car["name"],
            scac=car["scac"].strip(),
            days=days,
            tenders=tenders,
            acceptance_rate=weighted("acceptance_rate"),
            on_time_pickup_pct=weighted("on_time_pickup_pct"),
            on_time_delivery_pct=weighted("on_time_delivery_pct"),
            tracking_compliance_pct=weighted("tracking_compliance_pct"),
            daily=daily,
        )

    @app.get("/metrics/summary", response_model=FleetSummary, tags=["analytics"])
    def get_summary(conn: Db) -> FleetSummary:
        now = now_for(conn)
        return FleetSummary(fleet_time=now, **q.fleet_summary(conn, now))

    # -- live -------------------------------------------------------------------------------

    @app.websocket("/ws/alerts")
    async def ws_alerts(ws: WebSocket) -> None:
        await ws.accept()
        queue = broadcaster.subscribe()
        try:
            await ws.send_json({"type": "hello", "message": "subscribed to freight.alerts"})
            while True:
                message = await queue.get()
                await ws.send_text(message)
        except WebSocketDisconnect:
            pass
        finally:
            broadcaster.unsubscribe(queue)
            with contextlib.suppress(Exception):
                await ws.close()

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def index() -> str:
        return INDEX_HTML

    return app


def app_factory() -> FastAPI:
    """Uvicorn entrypoint: ``uvicorn freightpulse.api.app:app_factory --factory``."""
    return create_app()

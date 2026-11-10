"""SQLAlchemy Core table metadata.

The DDL source of truth is the Alembic migrations in ``alembic/versions`` (partitioned tables
need hand-written SQL). These Table objects mirror them for typed Core queries; a test asserts
the two stay in sync.
"""

from __future__ import annotations

from sqlalchemy import (
    REAL,
    BigInteger,
    Column,
    Date,
    DateTime,
    Double,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

TS = DateTime(timezone=True)

carriers = Table(
    "carriers",
    metadata,
    Column("carrier_id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("scac", String(4), nullable=False),
    Column("mc_number", Text, nullable=False),
    Column("created_at", TS, nullable=False),
)

customers = Table(
    "customers",
    metadata,
    Column("customer_id", Text, primary_key=True),
    Column("name", Text, nullable=False),
)

facilities = Table(
    "facilities",
    metadata,
    Column("facility_id", Text, primary_key=True),
    Column("name", Text, nullable=False),
    Column("city", Text, nullable=False),
    Column("state", String(2), nullable=False),
    Column("lat", Double, nullable=False),
    Column("lon", Double, nullable=False),
    Column("timezone", Text, nullable=False),
    Column("avg_dwell_min", Numeric(8, 2)),
)

lanes = Table(
    "lanes",
    metadata,
    Column("lane_id", BigInteger, primary_key=True),
    Column("origin_facility_id", Text, nullable=False),
    Column("dest_facility_id", Text, nullable=False),
    Column("distance_mi", Numeric(8, 1), nullable=False),
)

shipments = Table(
    "shipments",
    metadata,
    Column("shipment_id", Text, primary_key=True),
    Column("customer_id", Text, nullable=False),
    Column("carrier_id", Text),
    Column("lane_id", BigInteger, nullable=False),
    Column("mode", Text, nullable=False),
    Column("equipment", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("rate_usd", Numeric(12, 2)),
    Column("created_at", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

shipment_stops = Table(
    "shipment_stops",
    metadata,
    Column("shipment_id", Text, primary_key=True),
    Column("stop_sequence", SmallInteger, primary_key=True),
    Column("facility_id", Text, nullable=False),
    Column("stop_type", Text, nullable=False),
    Column("appt_start", TS, nullable=False),
    Column("appt_end", TS, nullable=False),
    Column("arrived_at", TS),
    Column("departed_at", TS),
)

shipment_events = Table(
    "shipment_events",
    metadata,
    Column("event_id", Uuid, primary_key=True),
    Column("shipment_id", Text, nullable=False),
    Column("event_type", Text, nullable=False),
    Column("event_time", TS, primary_key=True),
    Column("ingested_at", TS, nullable=False),
    Column("source", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
)

gps_pings = Table(
    "gps_pings",
    metadata,
    Column("event_id", Uuid, primary_key=True),
    Column("shipment_id", Text, nullable=False),
    Column("event_time", TS, primary_key=True),
    Column("lat", Double, nullable=False),
    Column("lon", Double, nullable=False),
    Column("speed_mph", REAL, nullable=False),
    Column("driver_status", Text, nullable=False),
    Column("ingested_at", TS, nullable=False),
)

shipment_current_state = Table(
    "shipment_current_state",
    metadata,
    Column("shipment_id", Text, primary_key=True),
    Column("status", Text, nullable=False),
    Column("last_lat", Double),
    Column("last_lon", Double),
    Column("last_ping_at", TS),
    Column("next_stop_sequence", SmallInteger),
    Column("eta", TS),
    Column("eta_confidence", REAL),
    Column("late_risk", Text, nullable=False),
    Column("miles_remaining", Numeric(8, 1)),
    Column("last_event_time", TS, nullable=False),
    Column("version", BigInteger, nullable=False),
    Column("updated_at", TS, nullable=False),
    Column("status_event_time", TS, nullable=False),
    Column("pre_exception_status", Text),
    Column("carrier_event_time", TS),
    Column("rate_event_time", TS),
    Column("ewma_speed_mph", Double),
    Column("speed_var", Double, nullable=False),
    Column("speed_samples", Integer, nullable=False),
    Column("hos_drive_hours", Double, nullable=False),
    Column("hos_rest_started_at", TS),
    Column("hos_last_status", Text),
    Column("late_risk_stop_sequence", SmallInteger),
    Column("suspect_lat", Double),
    Column("suspect_lon", Double),
    Column("suspect_at", TS),
)

alerts = Table(
    "alerts",
    metadata,
    Column("alert_id", Uuid, primary_key=True),
    Column("shipment_id", Text, nullable=False),
    Column("alert_type", Text, nullable=False),
    Column("severity", Text, nullable=False),
    Column("message", Text, nullable=False),
    Column("triggered_at", TS, nullable=False),
    Column("resolved_at", TS),
    Column("dedupe_key", Text, nullable=False),
    Column("details", JSONB, nullable=False),
    Column("updated_at", TS, nullable=False),
)

lane_metrics_hourly = Table(
    "lane_metrics_hourly",
    metadata,
    Column("lane_id", BigInteger, primary_key=True),
    Column("hour_bucket", TS, primary_key=True),
    Column("shipments_delivered", Integer, nullable=False),
    Column("on_time_pct", Numeric(5, 2)),
    Column("avg_transit_hours", Numeric(8, 2)),
    Column("p90_transit_hours", Numeric(8, 2)),
    Column("avg_detention_min", Numeric(8, 2)),
)

carrier_scorecard_daily = Table(
    "carrier_scorecard_daily",
    metadata,
    Column("carrier_id", Text, primary_key=True),
    Column("day", Date, primary_key=True),
    Column("tenders", Integer, nullable=False),
    Column("acceptance_rate", Numeric(5, 2)),
    Column("on_time_pickup_pct", Numeric(5, 2)),
    Column("on_time_delivery_pct", Numeric(5, 2)),
    Column("tracking_compliance_pct", Numeric(5, 2)),
)

parked_events = Table(
    "parked_events",
    metadata,
    Column("event_id", Uuid, primary_key=True),
    Column("shipment_id", Text, nullable=False),
    Column("topic", Text, nullable=False),
    Column("raw", Text, nullable=False),
    Column("first_seen_at", TS, nullable=False),
    Column("attempts", Integer, nullable=False),
)

stream_watermarks = Table(
    "stream_watermarks",
    metadata,
    Column("consumer_group", Text, primary_key=True),
    Column("topic", Text, primary_key=True),
    Column("partition", Integer, primary_key=True),
    Column("max_event_time", TS, nullable=False),
    Column("updated_at", TS, nullable=False),
)

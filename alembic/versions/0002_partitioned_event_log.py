"""Append-only event log and GPS pings, range-partitioned by day on event_time.

Partitions themselves are created by freightpulse.storage.partitions (7 days ahead, on demand
for late/early events, and dropped after the retention window). See ADR 0004.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-10
"""

from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE shipment_events (
            event_id    uuid NOT NULL,
            shipment_id text NOT NULL,
            event_type  text NOT NULL,
            event_time  timestamptz NOT NULL,
            ingested_at timestamptz NOT NULL DEFAULT now(),
            source      text NOT NULL,
            payload     jsonb NOT NULL,
            PRIMARY KEY (event_id, event_time)
        ) PARTITION BY RANGE (event_time);
        -- Timeline: WHERE shipment_id = ? ORDER BY event_time (case study 1).
        CREATE INDEX ix_events_shipment_time ON shipment_events (shipment_id, event_time);

        CREATE TABLE gps_pings (
            event_id      uuid NOT NULL,
            shipment_id   text NOT NULL,
            event_time    timestamptz NOT NULL,
            lat           double precision NOT NULL,
            lon           double precision NOT NULL,
            speed_mph     real NOT NULL,
            driver_status text NOT NULL,
            ingested_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (event_id, event_time)
        ) PARTITION BY RANGE (event_time);
        -- Breadcrumbs: WHERE shipment_id = ? AND event_time >= ? ORDER BY event_time.
        CREATE INDEX ix_pings_shipment_time ON gps_pings (shipment_id, event_time);
        -- Fleet-wide time-range scans: BRIN is tiny and rows arrive in time order (case study 2).
        CREATE INDEX ix_pings_time_brin ON gps_pings USING brin (event_time)
            WITH (pages_per_range = 32);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE gps_pings; DROP TABLE shipment_events;")

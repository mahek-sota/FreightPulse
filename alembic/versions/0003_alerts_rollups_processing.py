"""Alerts, analytics rollups, and stream-processing bookkeeping tables.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-10
"""

from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE alerts (
            alert_id     uuid PRIMARY KEY,
            shipment_id  text NOT NULL REFERENCES shipments ON DELETE CASCADE,
            alert_type   text NOT NULL CHECK (alert_type IN ('LATE_RISK', 'DETENTION',
                             'STALE_TRACKING')),
            severity     text NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
            message      text NOT NULL,
            triggered_at timestamptz NOT NULL,
            resolved_at  timestamptz,
            dedupe_key   text NOT NULL UNIQUE,
            details      jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at   timestamptz NOT NULL DEFAULT now()
        );
        -- Active alerts per shipment (bonus case study 5): resolved rows are never indexed.
        CREATE INDEX ix_alerts_active_shipment ON alerts (shipment_id) WHERE resolved_at IS NULL;
        -- GET /alerts newest first, optionally by type.
        CREATE INDEX ix_alerts_type_triggered ON alerts (alert_type, triggered_at DESC);
        CREATE INDEX ix_alerts_triggered ON alerts (triggered_at DESC);

        CREATE TABLE lane_metrics_hourly (
            lane_id              bigint NOT NULL REFERENCES lanes,
            hour_bucket          timestamptz NOT NULL,
            shipments_delivered  integer NOT NULL,
            on_time_pct          numeric(5, 2),
            avg_transit_hours    numeric(8, 2),
            p90_transit_hours    numeric(8, 2),
            avg_detention_min    numeric(8, 2),
            PRIMARY KEY (lane_id, hour_bucket)
        );

        CREATE TABLE carrier_scorecard_daily (
            carrier_id              text NOT NULL REFERENCES carriers,
            day                     date NOT NULL,
            tenders                 integer NOT NULL,
            acceptance_rate         numeric(5, 2),
            on_time_pickup_pct      numeric(5, 2),
            on_time_delivery_pct    numeric(5, 2),
            tracking_compliance_pct numeric(5, 2),
            PRIMARY KEY (carrier_id, day)
        );

        -- Durable parking for events whose shipment is not known yet (shipment.created may lag).
        -- Parking lets the processor commit offsets without losing the event; see ADR 0002.
        CREATE TABLE parked_events (
            event_id      uuid PRIMARY KEY,
            shipment_id   text NOT NULL,
            topic         text NOT NULL,
            raw           text NOT NULL,
            first_seen_at timestamptz NOT NULL DEFAULT now(),
            attempts      integer NOT NULL DEFAULT 0
        );
        CREATE INDEX ix_parked_shipment ON parked_events (shipment_id);
        CREATE INDEX ix_parked_first_seen ON parked_events (first_seen_at);

        -- Max event time per consumed partition: the fleet's event-time clock for the scanners.
        CREATE TABLE stream_watermarks (
            consumer_group text NOT NULL,
            topic          text NOT NULL,
            partition      integer NOT NULL,
            max_event_time timestamptz NOT NULL,
            updated_at     timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (consumer_group, topic, partition)
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE stream_watermarks;
        DROP TABLE parked_events;
        DROP TABLE carrier_scorecard_daily;
        DROP TABLE lane_metrics_hourly;
        DROP TABLE alerts;
        """
    )

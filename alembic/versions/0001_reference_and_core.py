"""Reference data and core shipment tables.

Revision ID: 0001
Revises:
Create Date: 2026-11-10
"""

from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE carriers (
            carrier_id  text PRIMARY KEY,
            name        text NOT NULL,
            scac        char(4) NOT NULL UNIQUE,
            mc_number   text NOT NULL,
            created_at  timestamptz NOT NULL DEFAULT now()
        );

        CREATE TABLE customers (
            customer_id text PRIMARY KEY,
            name        text NOT NULL
        );

        CREATE TABLE facilities (
            facility_id   text PRIMARY KEY,
            name          text NOT NULL,
            city          text NOT NULL,
            state         char(2) NOT NULL,
            lat           double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
            lon           double precision NOT NULL CHECK (lon BETWEEN -180 AND 180),
            timezone      text NOT NULL,
            avg_dwell_min numeric(8, 2)
        );

        CREATE TABLE lanes (
            lane_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            origin_facility_id text NOT NULL REFERENCES facilities,
            dest_facility_id   text NOT NULL REFERENCES facilities,
            distance_mi        numeric(8, 1) NOT NULL,
            UNIQUE (origin_facility_id, dest_facility_id)
        );

        CREATE TABLE shipments (
            shipment_id text PRIMARY KEY,
            customer_id text NOT NULL REFERENCES customers,
            carrier_id  text REFERENCES carriers,
            lane_id     bigint NOT NULL REFERENCES lanes,
            mode        text NOT NULL CHECK (mode IN ('FTL', 'LTL')),
            equipment   text NOT NULL CHECK (equipment IN ('dry_van', 'reefer', 'flatbed')),
            status      text NOT NULL CHECK (status IN ('CREATED', 'TENDERED', 'REJECTED',
                            'ACCEPTED', 'AT_PICKUP', 'IN_TRANSIT', 'AT_DELIVERY', 'DELIVERED',
                            'EXCEPTION')),
            rate_usd    numeric(12, 2),
            created_at  timestamptz NOT NULL,
            updated_at  timestamptz NOT NULL
        );
        -- FK lookups and the carrier filter on GET /shipments.
        CREATE INDEX ix_shipments_carrier ON shipments (carrier_id, shipment_id);
        CREATE INDEX ix_shipments_lane ON shipments (lane_id);

        CREATE TABLE shipment_stops (
            shipment_id   text NOT NULL REFERENCES shipments ON DELETE CASCADE,
            stop_sequence smallint NOT NULL CHECK (stop_sequence >= 1),
            facility_id   text NOT NULL REFERENCES facilities,
            stop_type     text NOT NULL CHECK (stop_type IN ('PICKUP', 'DELIVERY')),
            appt_start    timestamptz NOT NULL,
            appt_end      timestamptz NOT NULL,
            arrived_at    timestamptz,
            departed_at   timestamptz,
            PRIMARY KEY (shipment_id, stop_sequence),
            CHECK (appt_end >= appt_start)
        );
        -- Detention scanner: only stops a truck is currently sitting at.
        CREATE INDEX ix_stops_open_dwell ON shipment_stops (arrived_at)
            WHERE arrived_at IS NOT NULL AND departed_at IS NULL;
        -- Dwell/rollups by facility and arrival time.
        CREATE INDEX ix_stops_facility_arrived ON shipment_stops (facility_id, arrived_at);

        CREATE TABLE shipment_current_state (
            shipment_id          text PRIMARY KEY REFERENCES shipments ON DELETE CASCADE,
            status               text NOT NULL,
            last_lat             double precision,
            last_lon             double precision,
            last_ping_at         timestamptz,
            next_stop_sequence   smallint,
            eta                  timestamptz,
            eta_confidence       real,
            late_risk            text NOT NULL DEFAULT 'LOW'
                                 CHECK (late_risk IN ('LOW', 'MEDIUM', 'HIGH')),
            miles_remaining      numeric(8, 1),
            last_event_time      timestamptz NOT NULL,
            version              bigint NOT NULL DEFAULT 0,
            updated_at           timestamptz NOT NULL DEFAULT now(),
            -- Processor bookkeeping (beyond the spec's columns; see ADR 0003).
            status_event_time    timestamptz NOT NULL,
            pre_exception_status text,
            carrier_event_time   timestamptz,
            rate_event_time      timestamptz,
            ewma_speed_mph       double precision,
            speed_var            double precision NOT NULL DEFAULT 0,
            speed_samples        integer NOT NULL DEFAULT 0,
            hos_drive_hours      double precision NOT NULL DEFAULT 0,
            hos_rest_started_at  timestamptz,
            hos_last_status      text,
            late_risk_stop_sequence smallint
        );
        -- GET /shipments?status=... with keyset pagination on shipment_id.
        CREATE INDEX ix_state_status_id ON shipment_current_state (status, shipment_id);
        -- GET /shipments?late_risk=... and the at-risk count: only non-LOW rows are indexed.
        CREATE INDEX ix_state_at_risk ON shipment_current_state (late_risk, shipment_id)
            WHERE late_risk <> 'LOW';
        -- Stale-tracking scanner.
        CREATE INDEX ix_state_in_transit_ping ON shipment_current_state (last_ping_at)
            WHERE status = 'IN_TRANSIT';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE shipment_current_state;
        DROP TABLE shipment_stops;
        DROP TABLE shipments;
        DROP TABLE lanes;
        DROP TABLE facilities;
        DROP TABLE customers;
        DROP TABLE carriers;
        """
    )

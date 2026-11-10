"""Track the last rejected GPS fix so a bad anchor can be replaced (re-anchoring).

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-10
"""

from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE shipment_current_state
            ADD COLUMN suspect_lat double precision,
            ADD COLUMN suspect_lon double precision,
            ADD COLUMN suspect_at  timestamptz;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE shipment_current_state
            DROP COLUMN suspect_at, DROP COLUMN suspect_lon, DROP COLUMN suspect_lat;
        """
    )

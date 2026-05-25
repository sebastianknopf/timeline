"""Add stop_sequence to fact_stop_times.

Revision ID: 20260525_0002
Revises: 20260524_0001
Create Date: 2026-05-25 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260525_0002"
down_revision = "20260524_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fact_stop_times",
        sa.Column("stop_sequence", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )

    op.execute(
        """
        WITH ranked AS (
            SELECT
                ctid,
                row_number() OVER (
                    PARTITION BY instance_id, operation_day_date, trip_id
                    ORDER BY nom_departure_time, distance_from_start, stop_id
                ) AS resolved_sequence
            FROM fact_stop_times
        )
        UPDATE fact_stop_times AS target
        SET stop_sequence = ranked.resolved_sequence
        FROM ranked
        WHERE target.ctid = ranked.ctid
        """
    )

    op.alter_column("fact_stop_times", "stop_sequence", server_default=None)

    op.create_index(
        "ix_fst_inst_opday_trip_seq",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "trip_id", "stop_sequence"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_fst_inst_opday_trip_seq",
        table_name="fact_stop_times",
    )
    op.drop_column("fact_stop_times", "stop_sequence")

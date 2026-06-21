"""Add realtime_assignment_method to dim_trips.

Revision ID: 20260621_0002
Revises: 20260527_0001
Create Date: 2026-06-21 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260621_0002"
down_revision = "20260527_0001"
branch_labels = None
depends_on = None


_CHECK_NAME = "ck_dim_trips_realtime_assignment_method"


def upgrade() -> None:
    op.add_column(
        "dim_trips",
        sa.Column("realtime_assignment_method", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        _CHECK_NAME,
        "dim_trips",
        "realtime_assignment_method IS NULL OR realtime_assignment_method IN ('DIRECT', 'MATCHING')",
    )


def downgrade() -> None:
    op.drop_constraint(_CHECK_NAME, "dim_trips", type_="check")
    op.drop_column("dim_trips", "realtime_assignment_method")

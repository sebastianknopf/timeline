"""Add loaded trip counters to fact_requests.

Revision ID: 20260627_0005
Revises: 20260623_0004
Create Date: 2026-06-27 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260627_0005"
down_revision = "20260623_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "fact_requests",
        sa.Column(
            "loaded_direct_trip_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "fact_requests",
        sa.Column(
            "loaded_matched_trip_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )


def downgrade() -> None:
    op.drop_column("fact_requests", "loaded_matched_trip_count")
    op.drop_column("fact_requests", "loaded_direct_trip_count")

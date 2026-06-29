"""Add realtime_pipeline_id to dim_trips.

Revision ID: 20260629_0005
Revises: 20260623_0004
Create Date: 2026-06-29 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260629_0005"
down_revision = "20260623_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dim_trips",
        sa.Column("realtime_pipeline_id", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dim_trips", "realtime_pipeline_id")

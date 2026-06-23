"""Populate dim_issue_types with the documented quality issue catalog.

Revision ID: 20260623_0004
Revises: 20260623_0003
Create Date: 2026-06-23 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260623_0004"
down_revision = "20260623_0003"
branch_labels = None
depends_on = None


_ISSUE_ROWS = [
    (1, "OperatorIdIsNull"),
    (2, "RouteIdIsNull"),
    (3, "OperationDayIsNull"),
    (4, "RouteIdNonGlobal"),
    (5, "StopIdNonGlobal"),
    (6, "TripIdNonGlobal"),
    (7, "TripNotMonitored"),
    (8, "TripPredictionInaccurate"),
    (9, "StartStopIdNull"),
    (10, "DestinationStopIdNull"),
    (11, "NotCompleteStopSequence"),
    (12, "NoNominalTripFound"),
    (13, "NoAmbiguousNominalTripFound"),
    (14, "AimedDepartureTimeBeforeArrivalTime"),
    (15, "EstimatedDepatureTimeBeforeArrivalTime"),
]


def upgrade() -> None:
    op.bulk_insert(
        sa.table(
            "dim_issue_types",
            sa.column("id", sa.Integer),
            sa.column("code", sa.Text),
        ),
        [{"id": issue_id, "code": code} for issue_id, code in _ISSUE_ROWS],
    )


def downgrade() -> None:
    issue_ids = [issue_id for issue_id, _ in _ISSUE_ROWS]
    op.execute(
        sa.text("DELETE FROM dim_issue_types WHERE id IN :issue_ids"),
        {"issue_ids": tuple(issue_ids)},
    )

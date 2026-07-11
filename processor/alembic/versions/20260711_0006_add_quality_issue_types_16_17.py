"""Add newly documented quality issue types to dim_issue_types.

Revision ID: 20260711_0006
Revises: 20260629_0005
Create Date: 2026-07-11 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260711_0006"
down_revision = "20260629_0005"
branch_labels = None
depends_on = None


_ISSUE_ROWS = [
    (16, "UnexpectedStopFound"),
    (17, "ExpectedStopMissing"),
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
    op.execute(
        sa.text("DELETE FROM dim_issue_types WHERE id IN (16, 17)"),
    )

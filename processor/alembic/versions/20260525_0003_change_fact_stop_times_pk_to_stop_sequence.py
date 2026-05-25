"""Change fact_stop_times primary key from distance_from_start to stop_sequence.

Revision ID: 20260525_0003
Revises: 20260525_0002
Create Date: 2026-05-25 00:00:00

Removes orphaned realtime placeholder rows (where distance_from_start equals the integer
stop_sequence, a known artefact of the old realtime pipeline), then replaces the primary
key so that upserts are keyed on (instance_id, operation_day_date, trip_id, stop_id,
stop_sequence) instead of (..., distance_from_start).  This allows re-runs of the nominal
pipeline to correct distance_from_start values on existing rows rather than inserting
additional phantom rows.
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260525_0003"
down_revision = "20260525_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove rows inserted by the old realtime pipeline before nominal data was available.
    # Those rows have distance_from_start == stop_sequence (integer cast to float) which is
    # the placeholder that was used when no nominal baseline existed.
    op.execute(
        """
        DELETE FROM fact_stop_times
        WHERE distance_from_start = stop_sequence::double precision
          AND stop_sequence > 0
        """
    )

    # After the cleanup above there may still be duplicate (instance_id, operation_day_date,
    # trip_id, stop_id, stop_sequence) tuples in edge cases (e.g. stop_sequence backfilled as
    # 0 for old rows, or feeds with non-unique sequences).  Keep one row per group so the
    # new primary key can be created without a violation.
    op.execute(
        """
        DELETE FROM fact_stop_times
        WHERE ctid NOT IN (
            SELECT min(ctid)
            FROM fact_stop_times
            GROUP BY instance_id, operation_day_date, trip_id, stop_id, stop_sequence
        )
        """
    )

    # Drop old primary key.
    op.drop_constraint("pk_fact_stop_times", "fact_stop_times", type_="primary")

    # Create new primary key on stop_sequence instead of distance_from_start.
    op.create_primary_key(
        "pk_fact_stop_times",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "trip_id", "stop_id", "stop_sequence"],
    )


def downgrade() -> None:
    op.drop_constraint("pk_fact_stop_times", "fact_stop_times", type_="primary")

    op.create_primary_key(
        "pk_fact_stop_times",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "trip_id", "stop_id", "distance_from_start"],
    )

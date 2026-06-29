"""Add issue type and quality monitoring fact tables.

Revision ID: 20260623_0003
Revises: 20260621_0002
Create Date: 2026-06-23 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260623_0003"
down_revision = "20260621_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_issue_types",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_dim_issue_types"),
    )

    op.create_table(
        "fact_requests",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("pipeline_id", sa.Text(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("num_entities", sa.Integer(), nullable=False),
        sa.Column(
            "loaded_direct_trip_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "loaded_matched_trip_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("age_seconds", sa.Integer(), nullable=False),
        sa.Column(
            "status_code",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("200"),
        ),
        sa.PrimaryKeyConstraint("instance_id", "request_id", name="pk_fact_requests"),
    )
    op.create_index(
        "ix_fact_requests_instance_id_pipeline_id_timestamp",
        "fact_requests",
        ["instance_id", "pipeline_id", "timestamp"],
        unique=False,
    )

    op.create_table(
        "fact_quality_issues",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("issue_id", sa.Text(), nullable=False),
        sa.Column("pipeline_id", sa.Text(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entity_id", sa.Text(), nullable=False),
        sa.Column("issue_type_id", sa.Integer(), nullable=False),
        sa.Column("concessionaire_id", sa.Text(), nullable=True),
        sa.Column("concessionaire_name", sa.Text(), nullable=True),
        sa.Column("operator_id", sa.Text(), nullable=True),
        sa.Column("operator_name", sa.Text(), nullable=True),
        sa.Column("assessment_value", sa.Text(), nullable=True),
        sa.Column(
            "num_affected_values",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.ForeignKeyConstraint(
            ["issue_type_id"],
            ["dim_issue_types.id"],
            name="fk_fact_quality_issues_issue_type",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("instance_id", "issue_id", name="pk_fact_quality_issues"),
    )
    op.create_index(
        "ix_fact_quality_issues_on_issue_type",
        "fact_quality_issues",
        ["instance_id", "pipeline_id", "timestamp", "issue_type_id"],
        unique=False,
    )
    op.create_index(
        "ix_fact_quality_issues_on_operator",
        "fact_quality_issues",
        ["instance_id", "pipeline_id", "concessionaire_id", "operator_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_fact_quality_issues_on_operator",
        table_name="fact_quality_issues",
    )
    op.drop_index(
        "ix_fact_quality_issues_on_issue_type",
        table_name="fact_quality_issues",
    )
    op.drop_table("fact_quality_issues")

    op.drop_index(
        "ix_fact_requests_instance_id_pipeline_id_timestamp",
        table_name="fact_requests",
    )
    op.drop_table("fact_requests")

    op.drop_table("dim_issue_types")

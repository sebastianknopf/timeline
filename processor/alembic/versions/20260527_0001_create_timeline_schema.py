"""Create initial Timeline schema.

Revision ID: 20260527_0001
Revises: None
Create Date: 2026-05-27 00:00:00

"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260527_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dim_stops",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("stop_name", sa.Text(), nullable=False),
        sa.Column("stop_lat", sa.Double(), nullable=False),
        sa.Column("stop_lon", sa.Double(), nullable=False),
        sa.PrimaryKeyConstraint("instance_id", "stop_id", name="pk_dim_stops"),
    )
    op.create_index(
        "ix_dim_stops_instance_id_stop_name",
        "dim_stops",
        ["instance_id", "stop_name"],
        unique=False,
    )

    op.create_table(
        "dim_routes",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("route_name", sa.Text(), nullable=False),
        sa.Column("concessionaire_id", sa.Text(), nullable=True),
        sa.Column("concessionaire_name", sa.Text(), nullable=True),
        sa.Column("operator_id", sa.Text(), nullable=True),
        sa.Column("operator_name", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("instance_id", "route_id", name="pk_dim_routes"),
    )
    op.create_index(
        "ix_dim_routes_instance_id_concessionaire_id",
        "dim_routes",
        ["instance_id", "concessionaire_id"],
        unique=False,
    )
    op.create_index(
        "ix_dim_routes_instance_id_operator_id",
        "dim_routes",
        ["instance_id", "operator_id"],
        unique=False,
    )

    op.create_table(
        "dim_trips",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("operation_day_date", sa.Date(), nullable=False),
        sa.Column("trip_id", sa.Text(), nullable=False),
        sa.Column("route_id", sa.Text(), nullable=False),
        sa.Column("concessionaire_id", sa.Text(), nullable=True),
        sa.Column("concessionaire_name", sa.Text(), nullable=True),
        sa.Column("operator_id", sa.Text(), nullable=True),
        sa.Column("operator_name", sa.Text(), nullable=True),
        sa.Column("nom_start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("nom_end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("act_start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("act_end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("nom_start_stop_id", sa.Text(), nullable=False),
        sa.Column("nom_end_stop_id", sa.Text(), nullable=False),
        sa.Column("nom_total_distance", sa.Double(), nullable=False),
        sa.Column("act_total_distance", sa.Double(), nullable=True),
        sa.Column(
            "schedule_relationship",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'UNKNOWN'"),
        ),
        sa.ForeignKeyConstraint(
            ["instance_id", "route_id"],
            ["dim_routes.instance_id", "dim_routes.route_id"],
            name="fk_dim_trips_route",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["instance_id", "nom_start_stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_dim_trips_nom_start_stop",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["instance_id", "nom_end_stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_dim_trips_nom_end_stop",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "instance_id",
            "operation_day_date",
            "trip_id",
            name="pk_dim_trips",
        ),
    )
    op.create_index(
        "ix_dim_trips_instance_id_operation_day_date_route_id",
        "dim_trips",
        ["instance_id", "operation_day_date", "route_id"],
        unique=False,
    )
    op.create_index(
        "ix_dim_trips_instance_id_operation_day_date_operator_id",
        "dim_trips",
        ["instance_id", "operation_day_date", "operator_id"],
        unique=False,
    )
    op.create_index(
        "ix_dim_trips_instance_id_operation_day_date_concessionaire_id",
        "dim_trips",
        ["instance_id", "operation_day_date", "concessionaire_id"],
        unique=False,
    )

    op.create_table(
        "fact_stop_times",
        sa.Column("instance_id", sa.Text(), nullable=False),
        sa.Column("operation_day_date", sa.Date(), nullable=False),
        sa.Column("trip_id", sa.Text(), nullable=False),
        sa.Column("stop_id", sa.Text(), nullable=False),
        sa.Column("stop_sequence", sa.Integer(), nullable=False),
        sa.Column("distance_from_start", sa.Double(), nullable=False),
        sa.Column("nom_arrival_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("nom_departure_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("act_arrival_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("act_departure_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "schedule_relationship",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'UNKNOWN'"),
        ),
        sa.ForeignKeyConstraint(
            ["instance_id", "stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_fact_stop_times_stop",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["instance_id", "operation_day_date", "trip_id"],
            [
                "dim_trips.instance_id",
                "dim_trips.operation_day_date",
                "dim_trips.trip_id",
            ],
            name="fk_fact_stop_times_trip",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "instance_id",
            "operation_day_date",
            "trip_id",
            "stop_id",
            "stop_sequence",
            name="pk_fact_stop_times",
        ),
    )
    op.create_index(
        "ix_fact_stop_times_instance_id_operation_day_date_stop_id",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "stop_id"],
        unique=False,
    )
    op.create_index(
        "ix_fact_stop_times_instance_id_operation_day_date_trip_id",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "trip_id"],
        unique=False,
    )
    op.create_index(
        "ix_fst_inst_opday_trip_seq",
        "fact_stop_times",
        ["instance_id", "operation_day_date", "trip_id", "stop_sequence"],
        unique=False,
    )
    op.create_index(
        "ix_fact_stop_times_instance_id_act_arrival_time",
        "fact_stop_times",
        ["instance_id", "act_arrival_time"],
        unique=False,
    )
    op.create_index(
        "ix_fact_stop_times_instance_id_act_departure_time",
        "fact_stop_times",
        ["instance_id", "act_departure_time"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_fact_stop_times_instance_id_act_departure_time", table_name="fact_stop_times")
    op.drop_index("ix_fact_stop_times_instance_id_act_arrival_time", table_name="fact_stop_times")
    op.drop_index("ix_fst_inst_opday_trip_seq", table_name="fact_stop_times")
    op.drop_index("ix_fact_stop_times_instance_id_operation_day_date_trip_id", table_name="fact_stop_times")
    op.drop_index("ix_fact_stop_times_instance_id_operation_day_date_stop_id", table_name="fact_stop_times")
    op.drop_table("fact_stop_times")

    op.drop_index("ix_dim_trips_instance_id_operation_day_date_concessionaire_id", table_name="dim_trips")
    op.drop_index("ix_dim_trips_instance_id_operation_day_date_operator_id", table_name="dim_trips")
    op.drop_index("ix_dim_trips_instance_id_operation_day_date_route_id", table_name="dim_trips")
    op.drop_table("dim_trips")

    op.drop_index("ix_dim_routes_instance_id_operator_id", table_name="dim_routes")
    op.drop_index("ix_dim_routes_instance_id_concessionaire_id", table_name="dim_routes")
    op.drop_table("dim_routes")

    op.drop_index("ix_dim_stops_instance_id_stop_name", table_name="dim_stops")
    op.drop_table("dim_stops")

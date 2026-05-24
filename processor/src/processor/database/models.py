from __future__ import annotations

from datetime import date, datetime
from typing import ClassVar

from sqlalchemy import Date, DateTime, Double, ForeignKeyConstraint, Index, PrimaryKeyConstraint, Text, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base metadata for Timeline processor database models."""


class StopDimension(Base):
    __tablename__: ClassVar[str] = "dim_stops"
    __table_args__ = (
        PrimaryKeyConstraint("instance_id", "stop_id", name="pk_dim_stops"),
        Index("ix_dim_stops_instance_id_stop_name", "instance_id", "stop_name"),
    )

    instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    stop_id: Mapped[str] = mapped_column(Text, nullable=False)
    stop_name: Mapped[str] = mapped_column(Text, nullable=False)
    stop_lat: Mapped[float] = mapped_column(Double, nullable=False)
    stop_lon: Mapped[float] = mapped_column(Double, nullable=False)


class TripDimension(Base):
    __tablename__: ClassVar[str] = "dim_trips"
    __table_args__ = (
        PrimaryKeyConstraint(
            "instance_id",
            "operation_day_date",
            "trip_id",
            name="pk_dim_trips",
        ),
        ForeignKeyConstraint(
            ["instance_id", "nom_start_stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_dim_trips_nom_start_stop",
        ),
        ForeignKeyConstraint(
            ["instance_id", "nom_end_stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_dim_trips_nom_end_stop",
        ),
        Index(
            "ix_dim_trips_instance_id_operation_day_date_route_id",
            "instance_id",
            "operation_day_date",
            "route_id",
        ),
        Index(
            "ix_dim_trips_instance_id_operation_day_date_operator_id",
            "instance_id",
            "operation_day_date",
            "operator_id",
        ),
        Index(
            "ix_dim_trips_instance_id_operation_day_date_concessionaire_id",
            "instance_id",
            "operation_day_date",
            "concessionaire_id",
        ),
    )

    instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    operation_day_date: Mapped[date] = mapped_column(Date, nullable=False)
    trip_id: Mapped[str] = mapped_column(Text, nullable=False)
    route_id: Mapped[str] = mapped_column(Text, nullable=False)
    route_name: Mapped[str] = mapped_column(Text, nullable=False)
    concessionaire_id: Mapped[str] = mapped_column(Text, nullable=False)
    concessionaire_name: Mapped[str] = mapped_column(Text, nullable=False)
    operator_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    operator_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    nom_start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    nom_end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    act_start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    act_end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    nom_start_stop_id: Mapped[str] = mapped_column(Text, nullable=False)
    nom_end_stop_id: Mapped[str] = mapped_column(Text, nullable=False)
    nom_total_distance: Mapped[float] = mapped_column(Double, nullable=False)
    act_total_distance: Mapped[float | None] = mapped_column(Double, nullable=True)
    schedule_relationship: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'UNKNOWN'"),
    )


class StopTimeFact(Base):
    __tablename__: ClassVar[str] = "fact_stop_times"
    __table_args__ = (
        PrimaryKeyConstraint(
            "instance_id",
            "operation_day_date",
            "trip_id",
            "stop_id",
            "distance_from_start",
            name="pk_fact_stop_times",
        ),
        ForeignKeyConstraint(
            ["instance_id", "stop_id"],
            ["dim_stops.instance_id", "dim_stops.stop_id"],
            name="fk_fact_stop_times_stop",
        ),
        ForeignKeyConstraint(
            ["instance_id", "operation_day_date", "trip_id"],
            [
                "dim_trips.instance_id",
                "dim_trips.operation_day_date",
                "dim_trips.trip_id",
            ],
            name="fk_fact_stop_times_trip",
        ),
        Index(
            "ix_fact_stop_times_instance_id_operation_day_date_stop_id",
            "instance_id",
            "operation_day_date",
            "stop_id",
        ),
        Index(
            "ix_fact_stop_times_instance_id_operation_day_date_trip_id",
            "instance_id",
            "operation_day_date",
            "trip_id",
        ),
        Index(
            "ix_fact_stop_times_instance_id_act_arrival_time",
            "instance_id",
            "act_arrival_time",
        ),
        Index(
            "ix_fact_stop_times_instance_id_act_departure_time",
            "instance_id",
            "act_departure_time",
        ),
    )

    instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    operation_day_date: Mapped[date] = mapped_column(Date, nullable=False)
    trip_id: Mapped[str] = mapped_column(Text, nullable=False)
    stop_id: Mapped[str] = mapped_column(Text, nullable=False)
    distance_from_start: Mapped[float] = mapped_column(Double, nullable=False)
    nom_arrival_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    nom_departure_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    act_arrival_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    act_departure_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    schedule_relationship: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'UNKNOWN'"),
    )

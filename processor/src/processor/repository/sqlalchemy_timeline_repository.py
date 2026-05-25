from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import date
from typing import Callable, TypeVar

from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..database.models import StopDimension, StopTimeFact, TripDimension
from ..loading.models import StopRecord, StopTimeRecord, TripRecord
from .intf_timeline_repository import TimelineRepositoryInterface


# Batch sizes are derived from the stop-time budget:
# STOP_TIME_UPSERT_BATCH_SIZE * 10 params/row = 50,000 params per batch (PostgreSQL limit: 65,535).
# Each type's batch size is floor(50_000 / params_per_row), capped at STOP_TIME_UPSERT_BATCH_SIZE.
STOP_TIME_UPSERT_BATCH_SIZE = 5000  # 10 params/row; 5_000 * 10 = 50,000 params per batch
STOP_UPSERT_BATCH_SIZE = 5000       # 5 params/row;  floor(50_000 / 5) = 10_000 → capped at 5_000
TRIP_UPSERT_BATCH_SIZE = 2777       # 18 params/row; floor(50_000 / 18) = 2_777

TRecord = TypeVar("TRecord")


def _chunked_records(
    records: Sequence[TRecord],
    chunk_size: int,
) -> list[Sequence[TRecord]]:
    return [records[index : index + chunk_size] for index in range(0, len(records), chunk_size)]


class SqlAlchemyTimelineRepository(TimelineRepositoryInterface):
    """PostgreSQL-backed repository implementation for loading operations."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    async def upsert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await asyncio.to_thread(self._upsert_nominal_stops_sync, instance_id, stops)

    async def upsert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        await asyncio.to_thread(self._upsert_nominal_trips_sync, instance_id, trips)

    async def upsert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(self._upsert_nominal_stop_times_sync, instance_id, stop_times)

    async def insert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self.upsert_nominal_stops(instance_id=instance_id, stops=stops)

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.upsert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.upsert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        await asyncio.to_thread(self._upsert_realtime_trip_sync, instance_id, trip)

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(self._upsert_realtime_stop_times_sync, instance_id, stop_times)

    async def get_nominal_stop_times_for_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        return await asyncio.to_thread(
            self._get_nominal_stop_times_for_trip_sync,
            instance_id,
            operation_day_date,
            trip_id,
        )

    def _upsert_nominal_stops_sync(self, instance_id: str, stops: list[StopRecord]) -> None:
        if not stops:
            return

        table = StopDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                for stops_chunk in _chunked_records(stops, STOP_UPSERT_BATCH_SIZE):
                    rows = [
                        {
                            "instance_id": instance_id,
                            "stop_id": stop.stop_id,
                            "stop_name": stop.stop_name,
                            "stop_lat": stop.stop_lat,
                            "stop_lon": stop.stop_lon,
                        }
                        for stop in stops_chunk
                    ]
                    insert_stmt = postgresql_insert(table).values(rows)
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=["instance_id", "stop_id"],
                        set_={
                            "stop_name": insert_stmt.excluded.stop_name,
                            "stop_lat": insert_stmt.excluded.stop_lat,
                            "stop_lon": insert_stmt.excluded.stop_lon,
                        },
                    )
                    session.execute(upsert_stmt)

    def _upsert_nominal_trips_sync(self, instance_id: str, trips: list[TripRecord]) -> None:
        if not trips:
            return

        table = TripDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                for trips_chunk in _chunked_records(trips, TRIP_UPSERT_BATCH_SIZE):
                    rows = [self._trip_values(instance_id, trip) for trip in trips_chunk]
                    insert_stmt = postgresql_insert(table).values(rows)
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=["instance_id", "operation_day_date", "trip_id"],
                        set_={
                            "route_id": insert_stmt.excluded.route_id,
                            "route_name": insert_stmt.excluded.route_name,
                            "concessionaire_id": insert_stmt.excluded.concessionaire_id,
                            "concessionaire_name": insert_stmt.excluded.concessionaire_name,
                            "operator_id": insert_stmt.excluded.operator_id,
                            "operator_name": insert_stmt.excluded.operator_name,
                            "nom_start_time": insert_stmt.excluded.nom_start_time,
                            "nom_end_time": insert_stmt.excluded.nom_end_time,
                            "act_start_time": insert_stmt.excluded.act_start_time,
                            "act_end_time": insert_stmt.excluded.act_end_time,
                            "nom_start_stop_id": insert_stmt.excluded.nom_start_stop_id,
                            "nom_end_stop_id": insert_stmt.excluded.nom_end_stop_id,
                            "nom_total_distance": insert_stmt.excluded.nom_total_distance,
                            "act_total_distance": insert_stmt.excluded.act_total_distance,
                            "schedule_relationship": insert_stmt.excluded.schedule_relationship,
                        },
                    )
                    session.execute(upsert_stmt)

    def _upsert_nominal_stop_times_sync(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        if not stop_times:
            return

        table = StopTimeFact.__table__

        with self._session_factory() as session:
            with session.begin():
                for stop_times_chunk in _chunked_records(
                    stop_times,
                    STOP_TIME_UPSERT_BATCH_SIZE,
                ):
                    rows = [
                        self._stop_time_values(instance_id, stop_time)
                        for stop_time in stop_times_chunk
                    ]
                    insert_stmt = postgresql_insert(table).values(rows)
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=[
                            "instance_id",
                            "operation_day_date",
                            "trip_id",
                            "stop_id",
                            "stop_sequence",
                        ],
                        set_={
                            "distance_from_start": insert_stmt.excluded.distance_from_start,
                            "stop_sequence": insert_stmt.excluded.stop_sequence,
                            "nom_arrival_time": insert_stmt.excluded.nom_arrival_time,
                            "nom_departure_time": insert_stmt.excluded.nom_departure_time,
                            "act_arrival_time": insert_stmt.excluded.act_arrival_time,
                            "act_departure_time": insert_stmt.excluded.act_departure_time,
                            "schedule_relationship": insert_stmt.excluded.schedule_relationship,
                        },
                    )
                    session.execute(upsert_stmt)

    def _upsert_realtime_trip_sync(self, instance_id: str, trip: TripRecord) -> None:
        trip_values = self._trip_values(instance_id, trip)
        table = TripDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                insert_stmt = postgresql_insert(table).values(**trip_values)
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=["instance_id", "operation_day_date", "trip_id"],
                    set_={
                        "act_start_time": insert_stmt.excluded.act_start_time,
                        "act_end_time": insert_stmt.excluded.act_end_time,
                        "act_total_distance": insert_stmt.excluded.act_total_distance,
                        "schedule_relationship": insert_stmt.excluded.schedule_relationship,
                    },
                )
                session.execute(upsert_stmt)

    def _upsert_realtime_stop_times_sync(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        if not stop_times:
            return

        table = StopTimeFact.__table__

        with self._session_factory() as session:
            with session.begin():
                for stop_times_chunk in _chunked_records(
                    stop_times,
                    STOP_TIME_UPSERT_BATCH_SIZE,
                ):
                    rows = [
                        self._stop_time_values(instance_id, stop_time)
                        for stop_time in stop_times_chunk
                    ]
                    insert_stmt = postgresql_insert(table).values(rows)
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=[
                            "instance_id",
                            "operation_day_date",
                            "trip_id",
                            "stop_id",
                            "stop_sequence",
                        ],
                        set_={
                            "act_arrival_time": insert_stmt.excluded.act_arrival_time,
                            "act_departure_time": insert_stmt.excluded.act_departure_time,
                            "schedule_relationship": insert_stmt.excluded.schedule_relationship,
                        },
                    )
                    session.execute(upsert_stmt)

    def _get_nominal_stop_times_for_trip_sync(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        with self._session_factory() as session:
            stmt = (
                select(StopTimeFact)
                .where(StopTimeFact.instance_id == instance_id)
                .where(StopTimeFact.operation_day_date == operation_day_date)
                .where(StopTimeFact.trip_id == trip_id)
                .order_by(
                    StopTimeFact.nom_departure_time,
                    StopTimeFact.stop_sequence,
                    StopTimeFact.distance_from_start,
                    StopTimeFact.stop_id,
                )
            )
            rows = list(session.execute(stmt).scalars())

        return [
            StopTimeRecord(
                operation_day_date=row.operation_day_date,
                trip_id=row.trip_id,
                stop_id=row.stop_id,
                distance_from_start=row.distance_from_start,
                nom_arrival_time=row.nom_arrival_time,
                nom_departure_time=row.nom_departure_time,
                act_arrival_time=row.act_arrival_time,
                act_departure_time=row.act_departure_time,
                schedule_relationship=row.schedule_relationship,
                stop_sequence=row.stop_sequence,
            )
            for row in rows
        ]

    def _trip_values(self, instance_id: str, trip: TripRecord) -> dict[str, object]:
        return {
            "instance_id": instance_id,
            "operation_day_date": trip.operation_day_date,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "route_name": trip.route_name,
            "concessionaire_id": trip.concessionaire_id,
            "concessionaire_name": trip.concessionaire_name,
            "operator_id": trip.operator_id,
            "operator_name": trip.operator_name,
            "nom_start_time": trip.nom_start_time,
            "nom_end_time": trip.nom_end_time,
            "act_start_time": trip.act_start_time,
            "act_end_time": trip.act_end_time,
            "nom_start_stop_id": trip.nom_start_stop_id,
            "nom_end_stop_id": trip.nom_end_stop_id,
            "nom_total_distance": trip.nom_total_distance,
            "act_total_distance": trip.act_total_distance,
            "schedule_relationship": trip.schedule_relationship,
        }

    def _stop_time_values(
        self,
        instance_id: str,
        stop_time: StopTimeRecord,
    ) -> dict[str, object]:
        return {
            "instance_id": instance_id,
            "operation_day_date": stop_time.operation_day_date,
            "trip_id": stop_time.trip_id,
            "stop_id": stop_time.stop_id,
            "stop_sequence": stop_time.stop_sequence,
            "distance_from_start": stop_time.distance_from_start,
            "nom_arrival_time": stop_time.nom_arrival_time,
            "nom_departure_time": stop_time.nom_departure_time,
            "act_arrival_time": stop_time.act_arrival_time,
            "act_departure_time": stop_time.act_departure_time,
            "schedule_relationship": stop_time.schedule_relationship,
        }

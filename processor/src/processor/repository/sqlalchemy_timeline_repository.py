from __future__ import annotations

import asyncio
from typing import Callable

from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.orm import Session

from ..database.models import StopDimension, StopTimeFact, TripDimension
from ..loading.models import StopRecord, StopTimeRecord, TripRecord
from .intf_timeline_repository import TimelineRepositoryInterface


class SqlAlchemyTimelineRepository(TimelineRepositoryInterface):
    """PostgreSQL-backed repository implementation for loading operations."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self._session_factory = session_factory

    async def insert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await asyncio.to_thread(self._insert_nominal_stops_sync, instance_id, stops)

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(
            self._insert_nominal_trip_with_stop_times_sync,
            instance_id,
            trip,
            stop_times,
        )

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        await asyncio.to_thread(self._upsert_realtime_trip_sync, instance_id, trip)

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(self._upsert_realtime_stop_times_sync, instance_id, stop_times)

    def _insert_nominal_stops_sync(self, instance_id: str, stops: list[StopRecord]) -> None:
        if not stops:
            return

        with self._session_factory() as session:
            with session.begin():
                session.add_all(
                    [
                        StopDimension(
                            instance_id=instance_id,
                            stop_id=stop.stop_id,
                            stop_name=stop.stop_name,
                            stop_lat=stop.stop_lat,
                            stop_lon=stop.stop_lon,
                        )
                        for stop in stops
                    ]
                )

    def _insert_nominal_trip_with_stop_times_sync(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        with self._session_factory() as session:
            with session.begin():
                session.add(self._build_trip_model(instance_id, trip))
                session.add_all(
                    [self._build_stop_time_model(instance_id, stop_time) for stop_time in stop_times]
                )

    def _upsert_realtime_trip_sync(self, instance_id: str, trip: TripRecord) -> None:
        trip_values = self._trip_values(instance_id, trip)
        table = TripDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                insert_stmt = postgresql_insert(table).values(**trip_values)
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

    def _upsert_realtime_stop_times_sync(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        if not stop_times:
            return

        table = StopTimeFact.__table__
        rows = [self._stop_time_values(instance_id, stop_time) for stop_time in stop_times]

        with self._session_factory() as session:
            with session.begin():
                insert_stmt = postgresql_insert(table).values(rows)
                upsert_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=[
                        "instance_id",
                        "operation_day_date",
                        "trip_id",
                        "stop_id",
                        "distance_from_start",
                    ],
                    set_={
                        "nom_arrival_time": insert_stmt.excluded.nom_arrival_time,
                        "nom_departure_time": insert_stmt.excluded.nom_departure_time,
                        "act_arrival_time": insert_stmt.excluded.act_arrival_time,
                        "act_departure_time": insert_stmt.excluded.act_departure_time,
                        "schedule_relationship": insert_stmt.excluded.schedule_relationship,
                    },
                )
                session.execute(upsert_stmt)

    def _build_trip_model(self, instance_id: str, trip: TripRecord) -> TripDimension:
        return TripDimension(**self._trip_values(instance_id, trip))

    def _build_stop_time_model(self, instance_id: str, stop_time: StopTimeRecord) -> StopTimeFact:
        return StopTimeFact(**self._stop_time_values(instance_id, stop_time))

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
            "distance_from_start": stop_time.distance_from_start,
            "nom_arrival_time": stop_time.nom_arrival_time,
            "nom_departure_time": stop_time.nom_departure_time,
            "act_arrival_time": stop_time.act_arrival_time,
            "act_departure_time": stop_time.act_departure_time,
            "schedule_relationship": stop_time.schedule_relationship,
        }

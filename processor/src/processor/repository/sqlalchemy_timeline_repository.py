from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import date
from typing import Callable, TypeVar

from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ..database.models import RouteDimension, StopDimension, StopTimeFact, TripDimension
from ..exports.models import (
    ExportDataSet,
    ExportRouteRow,
    ExportStopRow,
    ExportStopTimeRow,
    ExportTripRow,
)
from ..loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord
from .intf_timeline_repository import TimelineRepositoryInterface


# Batch sizes are derived from the stop-time budget:
# STOP_TIME_UPSERT_BATCH_SIZE * 10 params/row = 50,000 params per batch (PostgreSQL limit: 65,535).
# Each type's batch size is floor(50_000 / params_per_row), capped at STOP_TIME_UPSERT_BATCH_SIZE.
STOP_TIME_UPSERT_BATCH_SIZE = 5000  # 10 params/row; 5_000 * 10 = 50,000 params per batch
STOP_UPSERT_BATCH_SIZE = 5000       # 5 params/row;  floor(50_000 / 5) = 10_000 → capped at 5_000
ROUTE_UPSERT_BATCH_SIZE = 7142      # 7 params/row;  floor(50_000 / 7) = 7_142
TRIP_UPSERT_BATCH_SIZE = 2941       # 17 params/row; floor(50_000 / 17) = 2_941

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

    async def insert_nominal_routes(self, instance_id: str, routes: list[RouteRecord]) -> None:
        await asyncio.to_thread(self._insert_nominal_routes_sync, instance_id, routes)

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

    async def insert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        await asyncio.to_thread(self._insert_nominal_trips_sync, instance_id, trips)

    async def insert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(self._insert_nominal_stop_times_sync, instance_id, stop_times)

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.insert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.insert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        await asyncio.to_thread(self._upsert_realtime_trip_sync, instance_id, trip)

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await asyncio.to_thread(self._upsert_realtime_stop_times_sync, instance_id, stop_times)

    async def get_nominal_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> TripRecord | None:
        return await asyncio.to_thread(
            self._get_nominal_trip_sync,
            instance_id,
            operation_day_date,
            trip_id,
        )

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

    async def find_nominal_trip_id_by_properties(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str,
        scheduled_start_time_str: str,
    ) -> str | None:
        return await asyncio.to_thread(
            self._find_nominal_trip_id_by_properties_sync,
            instance_id,
            operation_day_date,
            route_id,
            scheduled_start_time_str,
        )

    async def get_export_dataset(
        self,
        instance_id: str,
        from_date: date,
        to_date: date,
    ) -> ExportDataSet:
        return await asyncio.to_thread(
            self._get_export_dataset_sync,
            instance_id,
            from_date,
            to_date,
        )

    def _get_export_dataset_sync(
        self,
        instance_id: str,
        from_date: date,
        to_date: date,
    ) -> ExportDataSet:
        with self._session_factory() as session:
            stop_time_orm = session.execute(
                select(StopTimeFact)
                .where(
                    StopTimeFact.instance_id == instance_id,
                    StopTimeFact.operation_day_date >= from_date,
                    StopTimeFact.operation_day_date < to_date,
                )
                .order_by(
                    StopTimeFact.operation_day_date,
                    StopTimeFact.trip_id,
                    StopTimeFact.stop_sequence,
                )
            ).scalars().all()

            trip_keys: set[tuple[date, str]] = {
                (r.operation_day_date, r.trip_id) for r in stop_time_orm
            }
            stop_ids_from_facts: set[str] = {r.stop_id for r in stop_time_orm}

            if trip_keys:
                all_trip_orm = session.execute(
                    select(TripDimension)
                    .where(
                        TripDimension.instance_id == instance_id,
                        TripDimension.operation_day_date >= from_date,
                        TripDimension.operation_day_date < to_date,
                    )
                    .order_by(TripDimension.operation_day_date, TripDimension.trip_id)
                ).scalars().all()
                trip_orm = [
                    t for t in all_trip_orm if (t.operation_day_date, t.trip_id) in trip_keys
                ]
            else:
                trip_orm = []

            route_ids: set[str] = {t.route_id for t in trip_orm}
            stop_ids_from_trips: set[str] = (
                {t.nom_start_stop_id for t in trip_orm}
                | {t.nom_end_stop_id for t in trip_orm}
            )
            all_stop_ids = stop_ids_from_facts | stop_ids_from_trips

            if all_stop_ids:
                stop_orm = session.execute(
                    select(StopDimension)
                    .where(
                        StopDimension.instance_id == instance_id,
                        StopDimension.stop_id.in_(all_stop_ids),
                    )
                    .order_by(StopDimension.stop_id)
                ).scalars().all()
            else:
                stop_orm = []

            if route_ids:
                route_orm = session.execute(
                    select(RouteDimension)
                    .where(
                        RouteDimension.instance_id == instance_id,
                        RouteDimension.route_id.in_(route_ids),
                    )
                    .order_by(RouteDimension.route_id)
                ).scalars().all()
            else:
                route_orm = []

            return ExportDataSet(
                stop_times=[
                    ExportStopTimeRow(
                        operation_day_date=r.operation_day_date,
                        trip_id=r.trip_id,
                        stop_id=r.stop_id,
                        stop_sequence=r.stop_sequence,
                        distance_from_start=r.distance_from_start,
                        nom_arrival_time=r.nom_arrival_time,
                        nom_departure_time=r.nom_departure_time,
                        act_arrival_time=r.act_arrival_time,
                        act_departure_time=r.act_departure_time,
                        schedule_relationship=r.schedule_relationship,
                    )
                    for r in stop_time_orm
                ],
                trips=[
                    ExportTripRow(
                        operation_day_date=t.operation_day_date,
                        trip_id=t.trip_id,
                        route_id=t.route_id,
                        concessionaire_id=t.concessionaire_id,
                        concessionaire_name=t.concessionaire_name,
                        operator_id=t.operator_id,
                        operator_name=t.operator_name,
                        nom_start_time=t.nom_start_time,
                        nom_end_time=t.nom_end_time,
                        act_start_time=t.act_start_time,
                        act_end_time=t.act_end_time,
                        nom_start_stop_id=t.nom_start_stop_id,
                        nom_end_stop_id=t.nom_end_stop_id,
                        nom_total_distance=t.nom_total_distance,
                        act_total_distance=t.act_total_distance,
                        schedule_relationship=t.schedule_relationship,
                    )
                    for t in trip_orm
                ],
                stops=[
                    ExportStopRow(
                        stop_id=s.stop_id,
                        stop_name=s.stop_name,
                        stop_lat=s.stop_lat,
                        stop_lon=s.stop_lon,
                    )
                    for s in stop_orm
                ],
                routes=[
                    ExportRouteRow(
                        route_id=r.route_id,
                        route_name=r.route_name,
                        concessionaire_id=r.concessionaire_id,
                        concessionaire_name=r.concessionaire_name,
                        operator_id=r.operator_id,
                        operator_name=r.operator_name,
                    )
                    for r in route_orm
                ],
            )

    def _insert_nominal_routes_sync(self, instance_id: str, routes: list[RouteRecord]) -> None:
        if not routes:
            return

        table = RouteDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                for routes_chunk in _chunked_records(routes, ROUTE_UPSERT_BATCH_SIZE):
                    rows = [
                        {
                            "instance_id": instance_id,
                            "route_id": route.route_id,
                            "route_name": route.route_name,
                            "concessionaire_id": route.concessionaire_id,
                            "concessionaire_name": route.concessionaire_name,
                            "operator_id": route.operator_id,
                            "operator_name": route.operator_name,
                        }
                        for route in routes_chunk
                    ]
                    insert_stmt = postgresql_insert(table).values(rows)
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=["instance_id", "route_id"],
                        set_={
                            "route_name": insert_stmt.excluded.route_name,
                            "concessionaire_id": insert_stmt.excluded.concessionaire_id,
                            "concessionaire_name": insert_stmt.excluded.concessionaire_name,
                            "operator_id": insert_stmt.excluded.operator_id,
                            "operator_name": insert_stmt.excluded.operator_name,
                        },
                    )
                    session.execute(upsert_stmt)

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

    def _insert_nominal_trips_sync(self, instance_id: str, trips: list[TripRecord]) -> None:
        if not trips:
            return

        table = TripDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                for trips_chunk in _chunked_records(trips, TRIP_UPSERT_BATCH_SIZE):
                    rows = [self._trip_values(instance_id, trip) for trip in trips_chunk]
                    insert_stmt = postgresql_insert(table).values(rows)
                    # On conflict: refresh route FK and all nom_* fields so that a
                    # trip first inserted by the realtime pipeline with placeholder values
                    # is corrected by the nominal import.
                    # act_* fields and schedule_relationship are intentionally excluded
                    # so that realtime enrichment already present in the database is
                    # never overwritten by a nominal import run.
                    upsert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=["instance_id", "operation_day_date", "trip_id"],
                        set_={
                            "route_id": insert_stmt.excluded.route_id,
                            "concessionaire_id": insert_stmt.excluded.concessionaire_id,
                            "concessionaire_name": insert_stmt.excluded.concessionaire_name,
                            "operator_id": insert_stmt.excluded.operator_id,
                            "operator_name": insert_stmt.excluded.operator_name,
                            "nom_start_time": insert_stmt.excluded.nom_start_time,
                            "nom_end_time": insert_stmt.excluded.nom_end_time,
                            "nom_start_stop_id": insert_stmt.excluded.nom_start_stop_id,
                            "nom_end_stop_id": insert_stmt.excluded.nom_end_stop_id,
                            "nom_total_distance": insert_stmt.excluded.nom_total_distance,
                        },
                    )
                    session.execute(upsert_stmt)

    def _insert_nominal_stop_times_sync(
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
                    # On conflict: refresh all nom_* fields so that placeholder nominal
                    # times written by the realtime pipeline are corrected by the
                    # nominal import.  act_* fields and schedule_relationship are
                    # intentionally excluded so that realtime enrichment already present
                    # in the database is never overwritten by a nominal import run.
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
                            "nom_arrival_time": insert_stmt.excluded.nom_arrival_time,
                            "nom_departure_time": insert_stmt.excluded.nom_departure_time,
                        },
                    )
                    session.execute(upsert_stmt)

    def _upsert_realtime_trip_sync(self, instance_id: str, trip: TripRecord) -> None:
        # Realtime data only updates existing trips loaded by the nominal pipeline.
        # A pure UPDATE is used intentionally: if no matching dim_trips row exists
        # the statement is a silent no-op, preventing phantom rows and FK violations.
        table = TripDimension.__table__

        with self._session_factory() as session:
            with session.begin():
                stmt = (
                    update(table)
                    .where(table.c.instance_id == instance_id)
                    .where(table.c.operation_day_date == trip.operation_day_date)
                    .where(table.c.trip_id == trip.trip_id)
                    .values(
                        act_start_time=trip.act_start_time,
                        act_end_time=trip.act_end_time,
                        act_total_distance=trip.act_total_distance,
                        schedule_relationship=trip.schedule_relationship,
                    )
                )
                session.execute(stmt)

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

    def _get_nominal_trip_sync(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> TripRecord | None:
        with self._session_factory() as session:
            stmt = (
                select(TripDimension)
                .where(TripDimension.instance_id == instance_id)
                .where(TripDimension.operation_day_date == operation_day_date)
                .where(TripDimension.trip_id == trip_id)
            )
            row = session.execute(stmt).scalars().first()

        if row is None:
            return None

        return TripRecord(
            operation_day_date=row.operation_day_date,
            trip_id=row.trip_id,
            route_id=row.route_id,
            operator_id=row.operator_id,
            operator_name=row.operator_name,
            concessionaire_id=row.concessionaire_id,
            concessionaire_name=row.concessionaire_name,
            nom_start_time=row.nom_start_time,
            nom_end_time=row.nom_end_time,
            act_start_time=row.act_start_time,
            act_end_time=row.act_end_time,
            nom_start_stop_id=row.nom_start_stop_id,
            nom_end_stop_id=row.nom_end_stop_id,
            nom_total_distance=row.nom_total_distance,
            act_total_distance=row.act_total_distance,
            schedule_relationship=row.schedule_relationship,
        )

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

    def _find_nominal_trip_id_by_properties_sync(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str,
        scheduled_start_time_str: str,
    ) -> str | None:
        try:
            parts = scheduled_start_time_str.split(":")
            target_hour = int(parts[0]) % 24
            target_minute = int(parts[1])
        except (ValueError, IndexError):
            return None

        with self._session_factory() as session:
            rows = list(
                session.execute(
                    select(TripDimension.trip_id, TripDimension.nom_start_time)
                    .where(TripDimension.instance_id == instance_id)
                    .where(TripDimension.operation_day_date == operation_day_date)
                    .where(TripDimension.route_id == route_id)
                ).all()
            )

        matches = [
            trip_id
            for trip_id, nom_start_time in rows
            if nom_start_time.hour == target_hour and nom_start_time.minute == target_minute
        ]
        return matches[0] if len(matches) == 1 else None

    def _trip_values(self, instance_id: str, trip: TripRecord) -> dict[str, object]:
        return {
            "instance_id": instance_id,
            "operation_day_date": trip.operation_day_date,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
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

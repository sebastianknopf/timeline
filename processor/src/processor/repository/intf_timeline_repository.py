from __future__ import annotations

from datetime import date
from typing import Protocol

from ..exports.models import ExportDataSet
from ..loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord


class TimelineRepositoryInterface(Protocol):
    async def upsert_nominal_stops(
        self,
        instance_id: str,
        stops: list[StopRecord],
    ) -> None:
        """Upsert nominal stops for one instance."""

    async def insert_nominal_routes(
        self,
        instance_id: str,
        routes: list[RouteRecord],
    ) -> None:
        """Upsert nominal routes for one instance.

        On conflict (same instance_id, route_id) the route metadata is refreshed so that
        subsequent nominal pipeline runs can correct route names and concessionaire data.
        """

    async def upsert_nominal_trips(
        self,
        instance_id: str,
        trips: list[TripRecord],
    ) -> None:
        """Upsert nominal trips for one instance."""

    async def upsert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        """Upsert nominal stop times for one instance."""

    async def insert_nominal_stops(
        self,
        instance_id: str,
        stops: list[StopRecord],
    ) -> None:
        """Insert nominal stops for one instance."""

    async def insert_nominal_trips(
        self,
        instance_id: str,
        trips: list[TripRecord],
    ) -> None:
        """Insert nominal trips for one instance.

        Rows for trips that already exist (same instance_id, operation_day_date, trip_id) are
        silently skipped so that in-progress trips with realtime data are never overwritten.
        """

    async def insert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        """Insert nominal stop times for one instance.

        Rows for stop times that already exist are silently skipped so that in-progress stop
        times with realtime data are never overwritten.
        """

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        """Insert one nominal trip and related nominal stop times.

        Both the trip and every stop time row are only written when no matching row already
        exists, preserving any realtime enrichment that may already be present in the database.
        """

    async def upsert_realtime_trip(
        self,
        instance_id: str,
        trip: TripRecord,
    ) -> None:
        """Upsert trip data when realtime information arrives."""

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        """Upsert stop time rows when realtime information arrives."""

    async def get_nominal_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> TripRecord | None:
        """Read the stored nominal TripRecord for one trip and operation day.

        Returns the full TripRecord as it was written by the nominal pipeline, including the
        nom_total_distance that may have been derived from the shape index.  Returns None when
        no matching row exists in dim_trips.

        The loading service uses this to obtain the authoritative nom_total_distance when
        deriving realtime trip fields, because stop-level distance_from_start values may all be
        0.0 when the GTFS feed does not provide shape_dist_traveled in stop_times.txt.
        """

    async def get_nominal_stop_times_for_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        """Read nominal stop-time baseline for one trip and operation day."""

    async def find_nominal_trip_id_by_properties(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str,
        scheduled_start_time_str: str,
    ) -> str | None:
        """Find the nominal trip_id for a given route and scheduled start time on an operation day.

        Used as a secondary lookup when the primary trip_id from the realtime feed does not
        match any nominal trip row.  The lookup is based on route_id and the hour/minute of
        the nominal start time stored in dim_trips.  Returns the trip_id when exactly one match
        is found; returns None when no match or when the result is ambiguous.
        """

    async def get_export_dataset(
        self,
        instance_id: str,
        from_date: date,
        to_date: date,
    ) -> ExportDataSet:
        """Fetch timeline data for the given instance and half-open date interval [from_date, to_date).

        Queries fact_stop_times for the date range, then collects the minimum consistent set of
        referenced dim_trips, dim_stops, and dim_routes rows.  The instance_id column is not
        included in the returned row objects.
        """

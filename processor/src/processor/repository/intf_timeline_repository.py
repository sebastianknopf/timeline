from __future__ import annotations

from typing import Protocol

from ..loading.models import StopRecord, StopTimeRecord, TripRecord


class TimelineRepositoryInterface(Protocol):
    async def upsert_nominal_stops(
        self,
        instance_id: str,
        stops: list[StopRecord],
    ) -> None:
        """Upsert nominal stops for one instance."""

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

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        """Insert one nominal trip and related nominal stop times."""

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

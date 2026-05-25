from __future__ import annotations

from ..repository.intf_timeline_repository import TimelineRepositoryInterface
from .models import StopRecord, StopTimeRecord, TripRecord


class LoadingService:
    """Orchestrates loading while delegating persistence to repository interfaces."""

    def __init__(self, repository: TimelineRepositoryInterface) -> None:
        self._repository = repository

    async def load_nominal_stops_batch(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self._repository.upsert_nominal_stops(instance_id=instance_id, stops=stops)

    async def load_nominal_trips_batch(self, instance_id: str, trips: list[TripRecord]) -> None:
        await self._repository.upsert_nominal_trips(instance_id=instance_id, trips=trips)

    async def load_nominal_stop_times_batch(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self._repository.upsert_nominal_stop_times(
            instance_id=instance_id,
            stop_times=stop_times,
        )

    async def load_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self.load_nominal_stops_batch(instance_id=instance_id, stops=stops)

    async def load_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.load_nominal_trips_batch(instance_id=instance_id, trips=[trip])
        await self.load_nominal_stop_times_batch(instance_id=instance_id, stop_times=stop_times)

    async def load_realtime_trip_and_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        # Matching strategy hooks belong here while DB writes remain in the repository.
        await self._repository.upsert_realtime_trip(instance_id=instance_id, trip=trip)
        await self._repository.upsert_realtime_stop_times(
            instance_id=instance_id,
            stop_times=stop_times,
        )

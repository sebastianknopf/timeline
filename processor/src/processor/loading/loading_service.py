from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

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
        if not stop_times:
            return

        normalized_stop_times = self._normalize_realtime_stop_times(stop_times)
        if not normalized_stop_times:
            return

        normalized_trip = self._derive_realtime_trip_fields(
            source_trip=trip,
            stop_times=normalized_stop_times,
        )

        # Matching strategy hooks belong here while DB writes remain in the repository.
        await self._repository.upsert_realtime_trip(instance_id=instance_id, trip=normalized_trip)
        await self._repository.upsert_realtime_stop_times(
            instance_id=instance_id,
            stop_times=normalized_stop_times,
        )

    def _normalize_realtime_stop_times(self, stop_times: list[StopTimeRecord]) -> list[StopTimeRecord]:
        ordered = sorted(stop_times, key=lambda item: item.distance_from_start)
        timezone = ordered[0].nom_departure_time.tzinfo
        now = datetime.now(timezone)

        normalized: list[StopTimeRecord] = []
        for record in ordered:
            resolved_act_arrival = _resolve_realtime_timestamp(
                nominal_timestamp=record.nom_arrival_time,
                absolute_timestamp=record.act_arrival_time,
                delay_seconds=record.arrival_delay_seconds,
            )
            resolved_act_departure = _resolve_realtime_timestamp(
                nominal_timestamp=record.nom_departure_time,
                absolute_timestamp=record.act_departure_time,
                delay_seconds=record.departure_delay_seconds,
            )

            reference_arrival = resolved_act_arrival or record.nom_arrival_time
            reference_departure = resolved_act_departure or record.nom_departure_time
            if reference_arrival < now and reference_departure < now:
                continue

            normalized.append(
                replace(
                    record,
                    act_arrival_time=resolved_act_arrival,
                    act_departure_time=resolved_act_departure,
                )
            )

        return normalized

    def _derive_realtime_trip_fields(
        self,
        source_trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> TripRecord:
        ordered = sorted(stop_times, key=lambda item: item.distance_from_start)
        first_stop = ordered[0]
        last_stop = ordered[-1]

        nom_start_time = first_stop.nom_departure_time
        nom_end_time = last_stop.nom_arrival_time

        act_start_time = first_stop.act_departure_time or first_stop.act_arrival_time or nom_start_time

        end_candidate = last_stop.act_arrival_time or last_stop.act_departure_time or nom_end_time
        now = datetime.now(nom_end_time.tzinfo)
        act_end_time = end_candidate if end_candidate <= now else None

        return replace(
            source_trip,
            nom_start_time=nom_start_time,
            nom_end_time=nom_end_time,
            act_start_time=act_start_time,
            act_end_time=act_end_time,
            nom_start_stop_id=first_stop.stop_id,
            nom_end_stop_id=last_stop.stop_id,
            nom_total_distance=max(item.distance_from_start for item in ordered),
            act_total_distance=max(item.distance_from_start for item in ordered),
        )


def _resolve_realtime_timestamp(
    nominal_timestamp: datetime,
    absolute_timestamp: datetime | None,
    delay_seconds: int | None,
) -> datetime | None:
    if absolute_timestamp is not None:
        return absolute_timestamp

    if delay_seconds is not None:
        return nominal_timestamp + timedelta(seconds=delay_seconds)

    return None

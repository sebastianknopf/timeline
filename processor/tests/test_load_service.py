from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.common.quality_issues import QualityIssue
from processor.common.runtime_config_service import RuntimeConfigService
from processor.loading.loading_service import LoadingService, RealtimeLoadingQualityIssue, RealtimeLoadingResult
from processor.loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord
from processor.repository.intf_timeline_repository import TimelineRepositoryInterface
from processor.runtime_config import InstanceConfig, PipelineConfig, ProcessorConfig


class RecordingRepository(TimelineRepositoryInterface):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []
        self.realtime_trips: list[TripRecord] = []
        self.realtime_stop_times: list[StopTimeRecord] = []
        self.nominal_stop_times: list[StopTimeRecord] = []
        self.nominal_trips: list[TripRecord] = []
        # Controls find_nominal_trip_id_by_properties: key=(route_id, date, start_time, end_time, start_stop_id, end_stop_id)
        self.alternative_trip_id_lookup: dict[
            tuple[str | None, date, datetime | None, datetime | None, str | None, str | None],
            list[str],
        ] = {}

    async def upsert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        self.calls.append(("upsert_nominal_stops", instance_id, len(stops)))

    async def insert_nominal_routes(self, instance_id: str, routes: list[RouteRecord]) -> None:
        self.calls.append(("insert_nominal_routes", instance_id, len(routes)))

    async def upsert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        self.calls.append(("upsert_nominal_trips", instance_id, len(trips)))

    async def upsert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.calls.append(("upsert_nominal_stop_times", instance_id, len(stop_times)))

    async def insert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self.upsert_nominal_stops(instance_id=instance_id, stops=stops)

    async def insert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        self.calls.append(("insert_nominal_trips", instance_id, len(trips)))
        existing_by_key: dict[tuple[date, str], int] = {
            (t.operation_day_date, t.trip_id): i for i, t in enumerate(self.nominal_trips)
        }
        for trip in trips:
            key = (trip.operation_day_date, trip.trip_id)
            if key in existing_by_key:
                # Update route metadata and nom_* fields; preserve act_* and schedule_relationship.
                existing = self.nominal_trips[existing_by_key[key]]
                self.nominal_trips[existing_by_key[key]] = replace(
                    trip,
                    act_start_time=existing.act_start_time,
                    act_end_time=existing.act_end_time,
                    act_total_distance=existing.act_total_distance,
                    schedule_relationship=existing.schedule_relationship,
                )
            else:
                self.nominal_trips.append(trip)

    async def insert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.calls.append(("insert_nominal_stop_times", instance_id, len(stop_times)))
        existing_by_key: dict[tuple[date, str, str, int], int] = {
            (s.operation_day_date, s.trip_id, s.stop_id, s.stop_sequence): i
            for i, s in enumerate(self.nominal_stop_times)
        }
        for stop_time in stop_times:
            key = (stop_time.operation_day_date, stop_time.trip_id, stop_time.stop_id, stop_time.stop_sequence)
            if key in existing_by_key:
                # Update distance_from_start and nom_* fields; preserve act_* and schedule_relationship.
                existing = self.nominal_stop_times[existing_by_key[key]]
                self.nominal_stop_times[existing_by_key[key]] = replace(
                    stop_time,
                    act_arrival_time=existing.act_arrival_time,
                    act_departure_time=existing.act_departure_time,
                    schedule_relationship=existing.schedule_relationship,
                )
            else:
                self.nominal_stop_times.append(stop_time)

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.insert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.insert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        self.calls.append(("upsert_realtime_trip", instance_id, 1))
        self.realtime_trips.append(trip)

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.calls.append(("upsert_realtime_stop_times", instance_id, len(stop_times)))
        self.realtime_stop_times.extend(stop_times)

    async def get_nominal_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> TripRecord | None:
        for trip in self.nominal_trips:
            if trip.operation_day_date == operation_day_date and trip.trip_id == trip_id:
                return trip
        return None

    async def get_nominal_stop_times_for_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        return [
            item
            for item in self.nominal_stop_times
            if item.operation_day_date == operation_day_date and item.trip_id == trip_id
        ]

    async def find_nominal_trip_id_by_properties(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str | None,
        scheduled_start_time: datetime | None,
        scheduled_end_time: datetime | None,
        scheduled_start_stop_id: str | None,
        scheduled_end_stop_id: str | None,
    ) -> list[str] | None:
        return self.alternative_trip_id_lookup.get(
            (
                route_id,
                operation_day_date,
                scheduled_start_time,
                scheduled_end_time,
                scheduled_start_stop_id,
                scheduled_end_stop_id,
            )
        )


class LoadingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_nominal_stops_are_delegated_to_repository(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        stops = [
            StopRecord(stop_id="A", stop_name="Stop A", stop_lat=48.1, stop_lon=8.7),
            StopRecord(stop_id="B", stop_name="Stop B", stop_lat=48.2, stop_lon=8.8),
        ]

        await service.load_nominal_stops_batch(instance_id="demo", stops=stops)

        self.assertEqual([("upsert_nominal_stops", "demo", 2)], repository.calls)

    async def test_nominal_trip_already_existing_preserves_realtime_fields(self) -> None:
        """Nominal import must never overwrite realtime fields on a trip already in the database.

        A trip that was previously inserted and carries realtime enrichment
        (act_start_time, schedule_relationship, etc.) must retain those values when the
        nominal pipeline pushes the same trip again.  Route metadata and nom_* fields are
        allowed to be refreshed.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 5, 26)
        realtime_start = datetime(2026, 5, 26, 8, 5, tzinfo=UTC)

        # Simulate a trip that was already inserted and has since been enriched by the
        # realtime pipeline.
        existing_trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-existing",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_end_time=datetime(2026, 5, 26, 9, 0, tzinfo=UTC),
            act_start_time=realtime_start,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
            act_total_distance=None,
            schedule_relationship="SCHEDULED",
        )
        existing_stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-existing",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=datetime(2026, 5, 26, 8, 5, tzinfo=UTC),
            act_departure_time=datetime(2026, 5, 26, 8, 5, tzinfo=UTC),
            stop_sequence=1,
            schedule_relationship="SCHEDULED",
        )
        repository.nominal_trips = [existing_trip]
        repository.nominal_stop_times = [existing_stop_time]

        # Nominal pipeline pushes the same trip again (with no realtime data).
        nominal_trip_again = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-existing",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_end_time=datetime(2026, 5, 26, 9, 0, tzinfo=UTC),
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
            act_total_distance=None,
        )
        nominal_stop_time_again = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-existing",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=None,
            act_departure_time=None,
            stop_sequence=1,
        )

        await service.load_nominal_trips_batch(instance_id="demo", trips=[nominal_trip_again])
        await service.load_nominal_stop_times_batch(instance_id="demo", stop_times=[nominal_stop_time_again])

        # The existing trip's realtime enrichment must be fully intact.
        self.assertEqual(1, len(repository.nominal_trips))
        self.assertEqual(realtime_start, repository.nominal_trips[0].act_start_time)
        self.assertEqual("SCHEDULED", repository.nominal_trips[0].schedule_relationship)

        # The stop time's realtime timestamps must be unchanged.
        self.assertEqual(1, len(repository.nominal_stop_times))
        self.assertEqual(
            datetime(2026, 5, 26, 8, 5, tzinfo=UTC),
            repository.nominal_stop_times[0].act_arrival_time,
        )
        self.assertEqual("SCHEDULED", repository.nominal_stop_times[0].schedule_relationship)

    async def test_nominal_batch_preserves_realtime_fields_on_conflict(self) -> None:
        """load_nominal_trips_batch and load_nominal_stop_times_batch must preserve act_* fields.

        When a trip already exists with realtime enrichment, submitting the same trip via
        the batch methods must update nom/metadata fields but must NOT overwrite act_* fields
        or schedule_relationship.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 5, 26)
        realtime_arrival = datetime(2026, 5, 26, 8, 7, tzinfo=UTC)

        # Pre-populate with a trip and stop time that already carry realtime data.
        existing_trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-batch",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            act_start_time=datetime(2026, 5, 26, 8, 3, tzinfo=UTC),
            schedule_relationship="SCHEDULED",
        )
        existing_stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-batch",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=realtime_arrival,
            act_departure_time=realtime_arrival,
            stop_sequence=1,
            schedule_relationship="SCHEDULED",
        )
        repository.nominal_trips = [existing_trip]
        repository.nominal_stop_times = [existing_stop_time]

        # Nominal batch with correct route metadata.
        nominal_trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-batch",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_end_time=datetime(2026, 5, 26, 9, 0, tzinfo=UTC),
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
        )
        nominal_stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-batch",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=None,
            act_departure_time=None,
            stop_sequence=1,
        )

        await service.load_nominal_trips_batch(instance_id="demo", trips=[nominal_trip])
        await service.load_nominal_stop_times_batch(instance_id="demo", stop_times=[nominal_stop_time])

        # Route FK and nom_* fields must have been updated.
        self.assertEqual("route-1", repository.nominal_trips[0].route_id)
        self.assertEqual(datetime(2026, 5, 26, 8, 0, tzinfo=UTC), repository.nominal_trips[0].nom_start_time)

        # Realtime fields must remain untouched.
        self.assertEqual(datetime(2026, 5, 26, 8, 3, tzinfo=UTC), repository.nominal_trips[0].act_start_time)
        self.assertEqual("SCHEDULED", repository.nominal_trips[0].schedule_relationship)

        # Stop time nom fields must have been updated; act fields preserved.
        self.assertEqual(realtime_arrival, repository.nominal_stop_times[0].act_arrival_time)
        self.assertEqual("SCHEDULED", repository.nominal_stop_times[0].schedule_relationship)

    async def test_realtime_trip_is_rejected_when_current_pipeline_has_higher_priority(self) -> None:
        RuntimeConfigService.initialize(
            ProcessorConfig(
                instances=(
                    InstanceConfig(
                        id="demo",
                        pipelines=(
                            PipelineConfig(
                                id="high-priority-pipeline",
                                name="gtfsrt-high",
                                type="realtime",
                                cron="* * * * *",
                                endpoint="https://example.test/high",
                                priority=10,
                            ),
                            PipelineConfig(
                                id="low-priority-pipeline",
                                name="gtfsrt-low",
                                type="realtime",
                                cron="* * * * *",
                                endpoint="https://example.test/low",
                                priority=1,
                            ),
                        ),
                    ),
                )
            )
        )

        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 5, 26)
        trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-priority",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
            realtime_pipeline_id="high-priority-pipeline",
        )
        stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-priority",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=datetime(2026, 5, 26, 8, 2, tzinfo=UTC),
            act_departure_time=datetime(2026, 5, 26, 8, 2, tzinfo=UTC),
            stop_sequence=1,
        )
        repository.nominal_stop_times = [stop_time]
        repository.nominal_trips = [
            TripRecord(
                operation_day_date=operation_day,
                trip_id="trip-priority",
                route_id="route-1",
                operator_id="op-1",
                operator_name="Operator 1",
                realtime_pipeline_id="low-priority-pipeline",
            )
        ]

        result = await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=[stop_time],
        )

        self.assertEqual(RealtimeLoadingResult.FAIL_PIPELINE_PRIORITY, result)
        self.assertEqual([], repository.realtime_trips)

    async def test_realtime_trip_is_accepted_when_current_pipeline_has_lower_priority(self) -> None:
        RuntimeConfigService.initialize(
            ProcessorConfig(
                instances=(
                    InstanceConfig(
                        id="demo",
                        pipelines=(
                            PipelineConfig(
                                id="high-priority-pipeline",
                                name="gtfsrt-high",
                                type="realtime",
                                cron="* * * * *",
                                endpoint="https://example.test/high",
                                priority=10,
                            ),
                            PipelineConfig(
                                id="low-priority-pipeline",
                                name="gtfsrt-low",
                                type="realtime",
                                cron="* * * * *",
                                endpoint="https://example.test/low",
                                priority=1,
                            ),
                        ),
                    ),
                )
            )
        )

        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 5, 26)
        trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-priority-low",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
            realtime_pipeline_id="low-priority-pipeline",
        )
        stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-priority-low",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=datetime(2026, 5, 26, 8, 2, tzinfo=UTC),
            act_departure_time=datetime(2026, 5, 26, 8, 2, tzinfo=UTC),
            stop_sequence=1,
        )
        repository.nominal_stop_times = [stop_time]
        repository.nominal_trips = [
            TripRecord(
                operation_day_date=operation_day,
                trip_id="trip-priority-low",
                route_id="route-1",
                operator_id="op-1",
                operator_name="Operator 1",
                realtime_pipeline_id="high-priority-pipeline",
            )
        ]

        result = await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=[stop_time],
        )

        self.assertEqual(RealtimeLoadingResult.SUCCESS_DIRECT, result)
        self.assertEqual(1, len(repository.realtime_trips))

    async def test_realtime_trip_and_stop_times_are_delegated_to_repository(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        first_nom_arrival = now + timedelta(minutes=10)
        first_nom_departure = now + timedelta(minutes=11)
        last_nom_arrival = now + timedelta(minutes=45)
        last_nom_departure = now + timedelta(minutes=47)
        first_act_arrival = first_nom_arrival + timedelta(minutes=2)
        first_act_departure = first_nom_departure + timedelta(minutes=2)
        last_act_arrival = last_nom_arrival + timedelta(minutes=5)
        last_act_departure = last_nom_departure + timedelta(minutes=5)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-1",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-1",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=first_act_arrival,
                act_departure_time=first_act_departure,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-1",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=last_act_arrival,
                act_departure_time=last_act_departure,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = stop_times

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(
            [
                ("upsert_realtime_trip", "demo", 1),
                ("upsert_realtime_stop_times", "demo", 2),
            ],
            repository.calls,
        )
        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(first_act_departure, repository.realtime_trips[0].act_start_time)
        self.assertEqual(last_act_arrival, repository.realtime_trips[0].act_end_time)

    async def test_realtime_delay_fallback_and_trip_end_population(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        first_nom_arrival = now + timedelta(minutes=1)
        first_nom_departure = now + timedelta(minutes=2)
        last_nom_arrival = now - timedelta(minutes=2)
        last_nom_departure = now + timedelta(minutes=5)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-2",
            route_id="route-1",
            operator_id="op-1",
            operator_name="Operator 1",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-2",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                departure_delay_seconds=0,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-2",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                arrival_delay_seconds=300,
                departure_delay_seconds=-300,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = stop_times

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(first_nom_departure, repository.realtime_trips[0].act_start_time)
        self.assertEqual(last_nom_arrival + timedelta(seconds=300), repository.realtime_trips[0].act_end_time)

    async def test_realtime_syncs_arrival_and_departure_when_one_side_missing(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        first_nom_arrival = now + timedelta(minutes=5)
        first_nom_departure = now + timedelta(minutes=6)
        last_nom_arrival = now + timedelta(minutes=20)
        last_nom_departure = now + timedelta(minutes=21)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-3",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )

        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-3",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-3",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-3",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=None,
                act_departure_time=first_nom_departure + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-3",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=last_nom_arrival + timedelta(minutes=2),
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(2, len(repository.realtime_stop_times))
        by_sequence = {item.stop_sequence: item for item in repository.realtime_stop_times}
        self.assertEqual(by_sequence[1].act_departure_time, by_sequence[1].act_arrival_time)
        self.assertEqual(by_sequence[2].act_arrival_time, by_sequence[2].act_departure_time)

    async def test_realtime_past_stop_times_are_kept_for_upsert(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        first_nom_arrival = now - timedelta(minutes=30)
        first_nom_departure = now - timedelta(minutes=29)
        last_nom_arrival = now - timedelta(minutes=10)
        last_nom_departure = now - timedelta(minutes=9)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-4",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )

        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-4",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-4",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-4",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=first_nom_arrival,
                nom_departure_time=first_nom_departure,
                act_arrival_time=first_nom_arrival + timedelta(minutes=1),
                act_departure_time=first_nom_departure + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-4",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=last_nom_arrival,
                nom_departure_time=last_nom_departure,
                act_arrival_time=last_nom_arrival + timedelta(minutes=2),
                act_departure_time=last_nom_departure + timedelta(minutes=2),
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(2, len(repository.realtime_stop_times))
        self.assertEqual(first_nom_departure + timedelta(minutes=1), repository.realtime_trips[0].act_start_time)
        self.assertEqual(last_nom_arrival + timedelta(minutes=2), repository.realtime_trips[0].act_end_time)

    async def test_act_end_time_uses_arrival_when_both_available_for_last_stop(self) -> None:
        """act_end_time prefers act_arrival_time over act_departure_time on the last stop."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(minutes=30)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-arr",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-arr",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-arr",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        last_act_arrival = nom_base + timedelta(minutes=22)
        last_act_departure = nom_base + timedelta(minutes=25)
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-arr",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base,
                act_departure_time=nom_base + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-arr",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=last_act_arrival,
                act_departure_time=last_act_departure,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(last_act_arrival, repository.realtime_trips[0].act_end_time)

    async def test_act_end_time_falls_back_to_departure_when_arrival_absent_for_last_stop(self) -> None:
        """act_end_time falls back to act_departure_time when act_arrival_time is absent on the last stop."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(minutes=30)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-dep-fb",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-dep-fb",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-dep-fb",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        last_act_departure = nom_base + timedelta(minutes=23)
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-dep-fb",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base,
                act_departure_time=nom_base + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-dep-fb",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=None,
                act_departure_time=last_act_departure,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(last_act_departure, repository.realtime_trips[0].act_end_time)

    async def test_act_end_time_written_for_future_trip_last_stop(self) -> None:
        """act_end_time is written even when the last stop time is in the future."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        future_base = now + timedelta(hours=2)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-future",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-future",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=future_base,
                nom_departure_time=future_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-future",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=future_base + timedelta(minutes=30),
                nom_departure_time=future_base + timedelta(minutes=31),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        last_act_arrival = future_base + timedelta(minutes=32)
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-future",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=future_base,
                nom_departure_time=future_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=future_base + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-future",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=future_base + timedelta(minutes=30),
                nom_departure_time=future_base + timedelta(minutes=31),
                act_arrival_time=last_act_arrival,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertIsNotNone(repository.realtime_trips[0].act_end_time)
        self.assertEqual(last_act_arrival, repository.realtime_trips[0].act_end_time)

    async def test_act_start_time_uses_departure_and_falls_back_to_nom_departure(self) -> None:
        """act_start_time uses act_departure_time from the first stop; falls back to nom_departure_time."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now + timedelta(hours=1)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-start",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        first_act_departure = nom_base + timedelta(minutes=3)
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(minutes=2),
                act_departure_time=first_act_departure,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=nom_base + timedelta(minutes=17),
                act_departure_time=nom_base + timedelta(minutes=18),
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(first_act_departure, repository.realtime_trips[0].act_start_time)

    async def test_act_start_time_falls_back_to_nom_start_when_no_realtime_departure(self) -> None:
        """act_start_time falls back to nom_departure_time of first stop when no realtime departure is available."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now + timedelta(hours=1)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-start-fb",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start-fb",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start-fb",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        # Only arrival_delay for first stop — no departure information.
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start-fb",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(minutes=2),
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-start-fb",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # Since act_arrival and act_departure are mirrored when only one side is present,
        # act_departure_time of the first stop will equal act_arrival_time.
        self.assertEqual(nom_base + timedelta(minutes=2), repository.realtime_trips[0].act_start_time)

    async def test_act_start_time_uses_nom_departure_when_first_nominal_stop_has_no_realtime(self) -> None:
        """act_start_time falls back to the first nominal stop's nom_departure_time when that
        stop has no realtime coverage and no delay to propagate from.

        When the first nominal stop (S1) is absent from the realtime feed and no preceding
        update exists to propagate a delay forward, S1 is absent from normalized_stop_times.
        act_start_time must still be anchored to the first NOMINAL stop, falling back to
        nom_departure_time rather than using a middle stop's actual departure time.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now + timedelta(hours=1)
        nom_s1_arr = nom_base
        nom_s1_dep = nom_base + timedelta(minutes=1)
        nom_s2_arr = nom_base + timedelta(minutes=15)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_arr = nom_base + timedelta(minutes=30)
        nom_s3_dep = nom_base + timedelta(minutes=31)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-mid-start",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-mid-start",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_s1_arr,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-mid-start",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_s2_arr,
                nom_departure_time=nom_s2_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-mid-start",
                stop_id="C",
                distance_from_start=10.0,
                nom_arrival_time=nom_s3_arr,
                nom_departure_time=nom_s3_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]
        repository.nominal_stop_times = baseline

        s2_act_departure = nom_s2_dep + timedelta(minutes=2)
        # Only S2 is covered by the feed (no S1 update, no S3 update).
        # S1 has no preceding update and will be absent from normalized_stop_times.
        # S3 will be propagated from S2.
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-mid-start",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_s2_arr,
                nom_departure_time=nom_s2_dep,
                act_arrival_time=nom_s2_arr + timedelta(minutes=2),
                act_departure_time=s2_act_departure,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # act_start_time must be anchored to the first NOMINAL stop (S1) and fall back to
        # nom_departure_time because S1 has no realtime data — NOT the mid-trip S2 departure.
        self.assertEqual(nom_s1_dep, repository.realtime_trips[0].act_start_time)

    async def test_act_start_time_uses_first_nominal_stop_act_departure_when_available(self) -> None:
        """act_start_time uses act_departure_time of the first NOMINAL stop when it is present
        in the realtime feed, regardless of other stops that also have realtime data.

        When S1 has realtime coverage (act_departure_time set) and subsequent stops also
        have coverage, act_start_time must reflect S1's actual departure — not any later stop.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now + timedelta(hours=1)
        nom_s1_dep = nom_base + timedelta(minutes=1)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_dep = nom_base + timedelta(minutes=31)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-first-covered",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-first-covered",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-first-covered",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_s2_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-first-covered",
                stop_id="C",
                distance_from_start=10.0,
                nom_arrival_time=nom_base + timedelta(minutes=30),
                nom_departure_time=nom_s3_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]
        repository.nominal_stop_times = baseline

        s1_act_departure = nom_s1_dep + timedelta(minutes=3)
        s2_act_departure = nom_s2_dep + timedelta(minutes=2)

        # S1 and S2 both have realtime data; S3 absent (will be propagated from S2).
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-first-covered",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=nom_base + timedelta(minutes=2),
                act_departure_time=s1_act_departure,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-first-covered",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_s2_dep,
                act_arrival_time=nom_base + timedelta(minutes=17),
                act_departure_time=s2_act_departure,
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # act_start_time must use S1's actual departure, not S2's.
        self.assertEqual(s1_act_departure, repository.realtime_trips[0].act_start_time)

    async def test_act_end_time_uses_last_normalized_stop_not_last_nominal(self) -> None:
        """act_end_time is derived from the last stop in normalized_stop_times regardless of its schedule_relationship.

        When the feed delivers an update only for a middle stop, delay propagation fills
        the subsequent nominal stops as SCHEDULED.  act_end_time must use the propagated
        last stop's arrival time, not fall back to nom_end_time.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(hours=1)
        nom_s1_arr = nom_base
        nom_s1_dep = nom_base + timedelta(minutes=1)
        nom_s2_arr = nom_base + timedelta(minutes=15)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_arr = nom_base + timedelta(minutes=30)
        nom_s3_dep = nom_base + timedelta(minutes=31)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-prop-end",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-prop-end",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_s1_arr,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-prop-end",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_s2_arr,
                nom_departure_time=nom_s2_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-prop-end",
                stop_id="C",
                distance_from_start=10.0,
                nom_arrival_time=nom_s3_arr,
                nom_departure_time=nom_s3_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]
        repository.nominal_stop_times = baseline

        delay_s = 120
        # Only S1 delivered with delay; S2 and S3 will be propagated as SCHEDULED.
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-prop-end",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_s1_arr,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                arrival_delay_seconds=delay_s,
                departure_delay_seconds=delay_s,
                stop_sequence=1,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # S3 is propagated SCHEDULED; act_end_time must use its resolved arrival (nom + delay),
        # not nom_end_time.
        expected_act_end = nom_s3_arr + timedelta(seconds=delay_s)
        self.assertEqual(expected_act_end, repository.realtime_trips[0].act_end_time)

    async def test_non_added_trip_without_nominal_data_is_discarded(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="unknown-trip",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="unknown-trip",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=now + timedelta(minutes=5),
                nom_departure_time=now + timedelta(minutes=6),
                act_arrival_time=now + timedelta(minutes=5),
                act_departure_time=now + timedelta(minutes=6),
                stop_sequence=1,
            ),
        ]
        # No nominal_stop_times set → repository returns empty list for this trip_id.

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(0, len(repository.realtime_trips))
        self.assertEqual(0, len(repository.realtime_stop_times))

    async def test_added_trip_without_nominal_data_is_discarded(self) -> None:
        """ADDED trips that somehow reach the loading service are discarded like any other
        non-nominal trip, because ADDED support is not yet implemented in the system."""
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="added-trip",
            route_id="route-X",
            operator_id=None,
            operator_name=None,
            schedule_relationship="ADDED",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="added-trip",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=now + timedelta(minutes=5),
                nom_departure_time=now + timedelta(minutes=6),
                act_arrival_time=now + timedelta(minutes=5),
                act_departure_time=now + timedelta(minutes=6),
                stop_sequence=1,
            ),
        ]
        # No nominal_stop_times set; no alternative lookup configured → must be discarded.

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(0, len(repository.realtime_trips))
        self.assertEqual(0, len(repository.realtime_stop_times))

    async def test_non_added_trip_accepted_via_alternative_matching(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        op_day = now.date()
        nom_arrival = now + timedelta(minutes=10)
        nom_departure = now + timedelta(minutes=11)

        # Nominal data is stored under the canonical trip_id "nominal-T1".
        nominal_stop = StopTimeRecord(
            operation_day_date=op_day,
            trip_id="nominal-T1",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=nom_arrival,
            nom_departure_time=nom_departure,
            act_arrival_time=None,
            act_departure_time=None,
            stop_sequence=1,
        )
        repository.nominal_stop_times = [nominal_stop]
        scheduled_start = now + timedelta(minutes=10)
        scheduled_end = now + timedelta(minutes=40)
        scheduled_start_stop = "A"
        scheduled_end_stop = "B"
        repository.alternative_trip_id_lookup[
            (
                "route-1",
                op_day,
                scheduled_start,
                scheduled_end,
                scheduled_start_stop,
                scheduled_end_stop,
            )
        ] = ["nominal-T1"]

        # Realtime feed uses a different trip_id "feed-T1" that has no nominal entry.
        trip = TripRecord(
            operation_day_date=op_day,
            trip_id="feed-T1",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            _t_scheduled_start_time=scheduled_start,
            _t_scheduled_end_time=scheduled_end,
            _t_scheduled_start_stop_id=scheduled_start_stop,
            _t_scheduled_end_stop_id=scheduled_end_stop,
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=op_day,
                trip_id="feed-T1",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_arrival,
                nom_departure_time=nom_departure,
                act_arrival_time=nom_arrival + timedelta(minutes=2),
                act_departure_time=nom_departure + timedelta(minutes=2),
                stop_sequence=1,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        # Trip must be accepted and remapped to the nominal trip_id.
        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual("nominal-T1", repository.realtime_trips[0].trip_id)
        self.assertEqual(1, len(repository.realtime_stop_times))
        self.assertEqual("nominal-T1", repository.realtime_stop_times[0].trip_id)

    async def test_non_added_trip_discarded_when_alternative_matching_fails(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="feed-T2",
            route_id="route-2",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            _t_scheduled_start_time=datetime(year=2026, month=6, day=1, hour=9, minute=0, second=0),
            _t_scheduled_end_time=datetime(year=2026, month=6, day=1, hour=9, minute=45, second=0),
            _t_scheduled_start_stop_id="A",
            _t_scheduled_end_stop_id="B",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="feed-T2",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=now + timedelta(minutes=5),
                nom_departure_time=now + timedelta(minutes=6),
                act_arrival_time=now + timedelta(minutes=5),
                act_departure_time=now + timedelta(minutes=6),
                stop_sequence=1,
            ),
        ]
        # alternative_trip_id_lookup is empty → no alternative match found.

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(0, len(repository.realtime_trips))
        self.assertEqual(0, len(repository.realtime_stop_times))

    async def test_non_added_trip_discarded_when_no_scheduled_start_time(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="feed-T3",
            route_id="route-3",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            _t_scheduled_start_time=None,  # No start time → alternative matching impossible.
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="feed-T3",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=now + timedelta(minutes=5),
                nom_departure_time=now + timedelta(minutes=6),
                act_arrival_time=now + timedelta(minutes=5),
                act_departure_time=now + timedelta(minutes=6),
                stop_sequence=1,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(0, len(repository.realtime_trips))
        self.assertEqual(0, len(repository.realtime_stop_times))


    async def test_realtime_propagates_delay_to_nominal_stops_missing_from_feed(self) -> None:
        """LoadingService must expand nominal stops absent from the realtime feed using the
        last known effective delay, producing a complete realtime stop-time row for every
        nominal stop, not only those explicitly reported by the feed.

        Scenario: 4 nominal stops (S1-S4).  The feed delivers S1 (+120 s, absolute) and
        S3 (+300 s, absolute).  S2 must be propagated with S1's delay (+120 s); S4 must be
        propagated with S3's delay (+300 s).  This behaviour belongs to the loading service
        because only it has access to the complete nominal baseline.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        op_day = date(2026, 5, 30)
        nom_base = datetime(2026, 5, 30, 8, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=10)
        nom_s1_dep = nom_base + timedelta(minutes=11)
        nom_s2_arr = nom_base + timedelta(minutes=20)
        nom_s2_dep = nom_base + timedelta(minutes=21)
        nom_s3_arr = nom_base + timedelta(minutes=30)
        nom_s3_dep = nom_base + timedelta(minutes=31)
        nom_s4_arr = nom_base + timedelta(minutes=40)
        nom_s4_dep = nom_base + timedelta(minutes=41)

        # Nominal baseline — 4 stops.
        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-FILL", stop_id="S1",
                           distance_from_start=0.0, nom_arrival_time=nom_s1_arr,
                           nom_departure_time=nom_s1_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-FILL", stop_id="S2",
                           distance_from_start=2.0, nom_arrival_time=nom_s2_arr,
                           nom_departure_time=nom_s2_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=2),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-FILL", stop_id="S3",
                           distance_from_start=4.0, nom_arrival_time=nom_s3_arr,
                           nom_departure_time=nom_s3_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=3),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-FILL", stop_id="S4",
                           distance_from_start=6.0, nom_arrival_time=nom_s4_arr,
                           nom_departure_time=nom_s4_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=4),
        ]

        trip = TripRecord(
            operation_day_date=op_day,
            trip_id="T-FILL",
            route_id="R1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )

        # Feed only reports S1 (+120 s absolute) and S3 (+300 s absolute).
        # S2 and S4 are absent from the feed and must be synthesised by the loading service.
        feed_stop_times = [
            StopTimeRecord(
                operation_day_date=op_day,
                trip_id="T-FILL",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=nom_s1_arr,   # placeholder; overwritten by baseline
                nom_departure_time=nom_s1_dep,
                act_arrival_time=nom_s1_arr + timedelta(seconds=120),
                act_departure_time=nom_s1_dep + timedelta(seconds=120),
                schedule_relationship="SCHEDULED",
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=op_day,
                trip_id="T-FILL",
                stop_id="S3",
                distance_from_start=4.0,
                nom_arrival_time=nom_s3_arr,   # placeholder; overwritten by baseline
                nom_departure_time=nom_s3_dep,
                act_arrival_time=nom_s3_arr + timedelta(seconds=300),
                act_departure_time=nom_s3_dep + timedelta(seconds=300),
                schedule_relationship="SCHEDULED",
                stop_sequence=3,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=feed_stop_times,
        )

        # All 4 nominal stops must be persisted.
        self.assertEqual(4, len(repository.realtime_stop_times))

        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}

        # S1: explicit, +120 s.
        self.assertEqual(nom_s1_arr + timedelta(seconds=120), by_seq[1].act_arrival_time)
        self.assertEqual(nom_s1_dep + timedelta(seconds=120), by_seq[1].act_departure_time)

        # S2: propagated from S1 delay (+120 s), schedule_relationship = SCHEDULED.
        self.assertEqual(nom_s2_arr + timedelta(seconds=120), by_seq[2].act_arrival_time)
        self.assertEqual(nom_s2_dep + timedelta(seconds=120), by_seq[2].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[2].schedule_relationship)

        # S3: explicit, +300 s — resets propagation basis.
        self.assertEqual(nom_s3_arr + timedelta(seconds=300), by_seq[3].act_arrival_time)
        self.assertEqual(nom_s3_dep + timedelta(seconds=300), by_seq[3].act_departure_time)

        # S4: propagated from S3 delay (+300 s), schedule_relationship = SCHEDULED.
        self.assertEqual(nom_s4_arr + timedelta(seconds=300), by_seq[4].act_arrival_time)
        self.assertEqual(nom_s4_dep + timedelta(seconds=300), by_seq[4].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[4].schedule_relationship)

    async def test_realtime_propagates_delay_value_to_stops_missing_from_feed(self) -> None:
        """LoadingService propagates explicit delay_seconds values to nominal stops absent
        from the feed (delay-only case — no absolute timestamps in the feed).

        Scenario: 3 nominal stops.  The feed delivers only S1 (delay=+180 s).  S2 and S3
        are absent and must be synthesised with +180 s propagated.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        op_day = date(2026, 5, 30)
        nom_base = datetime(2026, 5, 30, 9, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=5)
        nom_s1_dep = nom_base + timedelta(minutes=6)
        nom_s2_arr = nom_base + timedelta(minutes=15)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_arr = nom_base + timedelta(minutes=25)
        nom_s3_dep = nom_base + timedelta(minutes=26)

        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-DELAY", stop_id="S1",
                           distance_from_start=0.0, nom_arrival_time=nom_s1_arr,
                           nom_departure_time=nom_s1_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-DELAY", stop_id="S2",
                           distance_from_start=3.0, nom_arrival_time=nom_s2_arr,
                           nom_departure_time=nom_s2_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=2),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-DELAY", stop_id="S3",
                           distance_from_start=6.0, nom_arrival_time=nom_s3_arr,
                           nom_departure_time=nom_s3_dep, act_arrival_time=None,
                           act_departure_time=None, stop_sequence=3),
        ]

        trip = TripRecord(
            operation_day_date=op_day,
            trip_id="T-DELAY",
            route_id="R1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )

        # Feed only has S1 with delay=+180 s; S2 and S3 are absent.
        feed_stop_times = [
            StopTimeRecord(
                operation_day_date=op_day,
                trip_id="T-DELAY",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=nom_s1_arr,
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                schedule_relationship="SCHEDULED",
                stop_sequence=1,
                arrival_delay_seconds=180,
                departure_delay_seconds=180,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=feed_stop_times,
        )

        # All 3 nominal stops must be persisted.
        self.assertEqual(3, len(repository.realtime_stop_times))

        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}

        # S1: explicit, delay=+180 s resolved.
        self.assertEqual(nom_s1_arr + timedelta(seconds=180), by_seq[1].act_arrival_time)
        self.assertEqual(nom_s1_dep + timedelta(seconds=180), by_seq[1].act_departure_time)

        # S2 and S3: propagated from S1 delay (+180 s), SCHEDULED.
        self.assertEqual(nom_s2_arr + timedelta(seconds=180), by_seq[2].act_arrival_time)
        self.assertEqual(nom_s2_dep + timedelta(seconds=180), by_seq[2].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[2].schedule_relationship)

        self.assertEqual(nom_s3_arr + timedelta(seconds=180), by_seq[3].act_arrival_time)
        self.assertEqual(nom_s3_dep + timedelta(seconds=180), by_seq[3].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[3].schedule_relationship)

    async def test_act_total_distance_equals_nom_total_distance_for_scheduled_trip(self) -> None:
        """For a SCHEDULED trip, act_total_distance must be set to nom_total_distance.

        A SCHEDULED trip is assumed to have operated its full nominal route, so the
        nominal total distance is attributed as the actual distance driven.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(hours=1)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-sched-dist",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-sched-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-sched-dist",
                stop_id="B",
                distance_from_start=12.5,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-sched-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(seconds=30),
                act_departure_time=nom_base + timedelta(minutes=1, seconds=30),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-sched-dist",
                stop_id="B",
                distance_from_start=12.5,
                nom_arrival_time=nom_base + timedelta(minutes=20),
                nom_departure_time=nom_base + timedelta(minutes=21),
                act_arrival_time=nom_base + timedelta(minutes=20, seconds=45),
                act_departure_time=nom_base + timedelta(minutes=21, seconds=45),
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertAlmostEqual(12.5, repository.realtime_trips[0].nom_total_distance)
        # SCHEDULED → act_total_distance must equal nom_total_distance.
        self.assertAlmostEqual(12.5, repository.realtime_trips[0].act_total_distance)

    async def test_act_total_distance_is_zero_for_unknown_trip(self) -> None:
        """For an UNKNOWN trip, act_total_distance must be 0.0.

        An UNKNOWN schedule_relationship means the trip's operational status cannot be
        determined; no actual kilometres are attributed.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(hours=1)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-unknown-dist",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="UNKNOWN",
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-unknown-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-unknown-dist",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-unknown-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(minutes=1),
                act_departure_time=nom_base + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-unknown-dist",
                stop_id="B",
                distance_from_start=8.0,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=nom_base + timedelta(minutes=15),
                act_departure_time=nom_base + timedelta(minutes=16),
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # UNKNOWN → act_total_distance must be 0.0.
        self.assertEqual(0.0, repository.realtime_trips[0].act_total_distance)

    async def test_act_total_distance_is_zero_for_cancelled_trip(self) -> None:
        """For a CANCELLED trip, act_total_distance must be 0.0.

        A CANCELLED trip did not operate, so no actual kilometres are attributed.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        nom_base = now - timedelta(hours=1)

        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="trip-cancelled-dist",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="CANCELLED",
        )
        baseline = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-cancelled-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-cancelled-dist",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=10),
                nom_departure_time=nom_base + timedelta(minutes=11),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        repository.nominal_stop_times = baseline

        # Even with actual times on stop-time records the trip-level act_total_distance
        # must be 0.0 because schedule_relationship is CANCELLED.
        realtime = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-cancelled-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base,
                act_departure_time=nom_base + timedelta(minutes=1),
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="trip-cancelled-dist",
                stop_id="B",
                distance_from_start=5.0,
                nom_arrival_time=nom_base + timedelta(minutes=10),
                nom_departure_time=nom_base + timedelta(minutes=11),
                act_arrival_time=nom_base + timedelta(minutes=10),
                act_departure_time=nom_base + timedelta(minutes=11),
                stop_sequence=2,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        # CANCELLED → act_total_distance must be 0.0.
        self.assertEqual(0.0, repository.realtime_trips[0].act_total_distance)

    async def test_act_total_distance_uses_nominal_trip_distance_when_stops_have_no_distances(self) -> None:
        """When all nominal stop times have distance_from_start = 0.0 but the stored nominal
        TripRecord carries a non-zero nom_total_distance (e.g. from the shape index), the
        loading service must use that stored value for both nom_total_distance and
        act_total_distance on a SCHEDULED trip.
        """
        now = datetime.now(UTC)
        nom_base = now.replace(hour=8, minute=0, second=0, microsecond=0)
        operation_day = nom_base.date()

        nominal_trip_record = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-shape-dist",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            nom_start_time=nom_base,
            nom_end_time=nom_base + timedelta(minutes=20),
            nom_start_stop_id="A",
            nom_end_stop_id="C",
            nom_total_distance=18.7,  # Stored from shape index; stops have no per-stop distances.
        )

        repository = RecordingRepository()
        repository.nominal_trips = [nominal_trip_record]
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-shape-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-shape-dist",
                stop_id="B",
                distance_from_start=0.0,
                nom_arrival_time=nom_base + timedelta(minutes=10),
                nom_departure_time=nom_base + timedelta(minutes=11),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-shape-dist",
                stop_id="C",
                distance_from_start=0.0,
                nom_arrival_time=nom_base + timedelta(minutes=19),
                nom_departure_time=nom_base + timedelta(minutes=20),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]

        service = LoadingService(repository=repository)

        trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-shape-dist",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )
        realtime = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-shape-dist",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(minutes=2),
                act_departure_time=nom_base + timedelta(minutes=2, seconds=30),
                stop_sequence=1,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        loaded = repository.realtime_trips[0]
        # nom_total_distance must reflect the shape-index value stored in dim_trips.
        self.assertAlmostEqual(18.7, loaded.nom_total_distance)
        # SCHEDULED + shape-index nom_total_distance → act_total_distance must be 18.7 too.
        self.assertAlmostEqual(18.7, loaded.act_total_distance)

    async def test_act_total_distance_falls_back_to_stop_level_when_no_nominal_trip_stored(self) -> None:
        """When get_nominal_trip returns None (no stored trip record) but nominal stop times
        have non-zero distance_from_start values, act_total_distance must be derived from
        the stop-level max as before.
        """
        now = datetime.now(UTC)
        nom_base = now.replace(hour=9, minute=0, second=0, microsecond=0)
        operation_day = nom_base.date()

        repository = RecordingRepository()
        # nominal_trips is empty → get_nominal_trip returns None.
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stopd",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stopd",
                stop_id="B",
                distance_from_start=9.3,
                nom_arrival_time=nom_base + timedelta(minutes=15),
                nom_departure_time=nom_base + timedelta(minutes=16),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]

        service = LoadingService(repository=repository)

        trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-stopd",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )
        realtime = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stopd",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=nom_base,
                nom_departure_time=nom_base + timedelta(minutes=1),
                act_arrival_time=nom_base + timedelta(minutes=1),
                act_departure_time=nom_base + timedelta(minutes=1, seconds=30),
                stop_sequence=1,
            ),
        ]

        await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=realtime,
        )

        self.assertEqual(1, len(repository.realtime_trips))
        loaded = repository.realtime_trips[0]
        # No nominal trip record → fall back to stop-level max = 9.3.
        self.assertAlmostEqual(9.3, loaded.nom_total_distance)
        self.assertAlmostEqual(9.3, loaded.act_total_distance)

    async def test_realtime_loading_callback_emits_unexpected_and_missing_stop_issues(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 6, 24)
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stop-issues",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=datetime(2026, 6, 24, 8, 0, tzinfo=UTC),
                nom_departure_time=datetime(2026, 6, 24, 8, 1, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stop-issues",
                stop_id="B",
                distance_from_start=1.0,
                nom_arrival_time=datetime(2026, 6, 24, 8, 2, tzinfo=UTC),
                nom_departure_time=datetime(2026, 6, 24, 8, 3, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]

        trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-stop-issues",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            _t_is_complete_stop_sequence=True,
        )

        stop_times = [
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stop-issues",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=datetime(2026, 6, 24, 8, 0, tzinfo=UTC),
                nom_departure_time=datetime(2026, 6, 24, 8, 1, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=operation_day,
                trip_id="trip-stop-issues",
                stop_id="X",
                distance_from_start=99.0,
                nom_arrival_time=datetime(2026, 6, 24, 8, 4, tzinfo=UTC),
                nom_departure_time=datetime(2026, 6, 24, 8, 5, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]

        reported_issues: list[RealtimeLoadingQualityIssue] = []

        result = await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
            issue_handler=reported_issues.append,
        )

        self.assertEqual(RealtimeLoadingResult.SUCCESS_DIRECT, result)
        self.assertEqual(
            [QualityIssue.UnexpectedStopFound, QualityIssue.ExpectedStopMissing],
            [issue.issue_type for issue in reported_issues],
        )
        self.assertEqual("X", reported_issues[0].assessment_value)
        self.assertEqual("B", reported_issues[1].assessment_value)

    async def test_realtime_loading_callback_emits_no_nominal_trip_issue(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 24),
            trip_id="trip-callback",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=date(2026, 6, 24),
                trip_id="trip-callback",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=datetime(2026, 6, 24, 8, 0, tzinfo=UTC),
                nom_departure_time=datetime(2026, 6, 24, 8, 1, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            )
        ]
        reported_issues: list[RealtimeLoadingQualityIssue] = []

        result = await service.load_realtime_trip_and_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
            issue_handler=reported_issues.append,
        )

        self.assertEqual(RealtimeLoadingResult.NO_NOMINAL_TRIP_FOUND, result)
        self.assertEqual(1, len(reported_issues))
        self.assertEqual(QualityIssue.NoNominalTripFound, reported_issues[0].issue_type)
        self.assertIsNotNone(reported_issues[0].assessment_value)
        self.assertEqual(f"trip_id={trip.trip_id}, route_id={trip.route_id}, realtime_start_stop_id={trip._t_scheduled_start_stop_id}, realtime_end_stop_id={trip._t_scheduled_end_stop_id}, realtime_start_time={trip._t_scheduled_start_time.isoformat() if trip._t_scheduled_start_time is not None else None}, realtime_end_time={trip._t_scheduled_end_time.isoformat() if trip._t_scheduled_end_time is not None else None}", reported_issues[0].assessment_value)


if __name__ == "__main__":
    unittest.main()

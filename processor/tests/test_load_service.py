from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.loading_service import LoadingService
from processor.loading.models import StopRecord, StopTimeRecord, TripRecord
from processor.repository.intf_timeline_repository import TimelineRepositoryInterface


class RecordingRepository(TimelineRepositoryInterface):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []
        self.realtime_trips: list[TripRecord] = []
        self.realtime_stop_times: list[StopTimeRecord] = []
        self.nominal_stop_times: list[StopTimeRecord] = []
        self.nominal_trips: list[TripRecord] = []
        # Controls find_nominal_trip_id_by_properties: key=(route_id, date, start_time_str)
        self.alternative_trip_id_lookup: dict[tuple[str, date, str], str] = {}

    async def upsert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        self.calls.append(("upsert_nominal_stops", instance_id, len(stops)))

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
        route_id: str,
        scheduled_start_time_str: str,
    ) -> str | None:
        return self.alternative_trip_id_lookup.get((route_id, operation_day_date, scheduled_start_time_str))


class LoadingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_nominal_stops_are_delegated_to_repository(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        stops = [
            StopRecord(stop_id="A", stop_name="Stop A", stop_lat=48.1, stop_lon=8.7),
            StopRecord(stop_id="B", stop_name="Stop B", stop_lat=48.2, stop_lon=8.8),
        ]

        await service.load_nominal_stops(instance_id="demo", stops=stops)

        self.assertEqual([("upsert_nominal_stops", "demo", 2)], repository.calls)

    async def test_nominal_trip_and_stop_times_are_delegated_to_repository(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        trip = TripRecord(
            operation_day_date=date(2026, 5, 24),
            trip_id="trip-1",
            route_id="route-1",
            route_name="Route 1",
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=datetime(2026, 5, 24, 8, 0, tzinfo=UTC),
            nom_end_time=datetime(2026, 5, 24, 9, 0, tzinfo=UTC),
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
            act_total_distance=None,
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=date(2026, 5, 24),
                trip_id="trip-1",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=datetime(2026, 5, 24, 8, 0, tzinfo=UTC),
                nom_departure_time=datetime(2026, 5, 24, 8, 1, tzinfo=UTC),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            )
        ]

        await service.load_nominal_trip_with_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(
            [
                ("insert_nominal_trips", "demo", 1),
                ("insert_nominal_stop_times", "demo", 1),
            ],
            repository.calls,
        )

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
            route_name="Route 1",
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
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
            route_name="Route 1",
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
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

        await service.load_nominal_trip_with_stop_times(
            instance_id="demo",
            trip=nominal_trip_again,
            stop_times=[nominal_stop_time_again],
        )

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

    async def test_nominal_trip_insert_corrects_route_name_set_by_realtime_pipeline(self) -> None:
        """Nominal import must correct route_name that was set to route_id by the realtime pipeline.

        When the realtime pipeline inserts a trip before the nominal data arrives, it sets
        route_name=route_id as a placeholder.  The subsequent nominal import run must update
        the route metadata (route_id, route_name, concessionaire, operator, nom_* fields)
        while leaving the realtime fields (act_*, schedule_relationship) untouched.
        """
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        operation_day = date(2026, 5, 26)
        realtime_start = datetime(2026, 5, 26, 8, 3, tzinfo=UTC)

        # Simulate what the realtime pipeline inserts when no nominal row exists yet:
        # route_name is set to the raw route_id value.
        realtime_inserted_trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-rt-first",
            route_id="RT-ROUTE-99",
            route_name="RT-ROUTE-99",  # placeholder — set to route_id by realtime pipeline
            operator_id=None,
            operator_name=None,
            act_start_time=realtime_start,
            schedule_relationship="SCHEDULED",
        )
        repository.nominal_trips = [realtime_inserted_trip]

        # Nominal pipeline now arrives with the correct human-readable route name.
        nominal_trip = TripRecord(
            operation_day_date=operation_day,
            trip_id="trip-rt-first",
            route_id="RT-ROUTE-99",
            route_name="Line 99 — City Express",
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_end_time=datetime(2026, 5, 26, 9, 0, tzinfo=UTC),
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=20.0,
        )
        nominal_stop_time = StopTimeRecord(
            operation_day_date=operation_day,
            trip_id="trip-rt-first",
            stop_id="A",
            distance_from_start=0.0,
            nom_arrival_time=datetime(2026, 5, 26, 8, 0, tzinfo=UTC),
            nom_departure_time=datetime(2026, 5, 26, 8, 1, tzinfo=UTC),
            act_arrival_time=None,
            act_departure_time=None,
            stop_sequence=1,
        )

        await service.load_nominal_trip_with_stop_times(
            instance_id="demo",
            trip=nominal_trip,
            stop_times=[nominal_stop_time],
        )

        # The route_name must now contain the correct human-readable name, not the route_id.
        self.assertEqual(1, len(repository.nominal_trips))
        self.assertEqual("Line 99 — City Express", repository.nominal_trips[0].route_name)
        self.assertEqual("Concessionaire 1", repository.nominal_trips[0].concessionaire_name)
        self.assertEqual("op-1", repository.nominal_trips[0].operator_id)
        self.assertEqual(datetime(2026, 5, 26, 8, 0, tzinfo=UTC), repository.nominal_trips[0].nom_start_time)

        # The realtime enrichment on the trip must still be intact.
        self.assertEqual(realtime_start, repository.nominal_trips[0].act_start_time)
        self.assertEqual("SCHEDULED", repository.nominal_trips[0].schedule_relationship)

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
            route_name="route-1",  # wrong — as if realtime pipeline inserted it first
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
            route_name="Correct Route Name",
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
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

        # Route metadata must have been updated.
        self.assertEqual("Correct Route Name", repository.nominal_trips[0].route_name)
        self.assertEqual("Concessionaire 1", repository.nominal_trips[0].concessionaire_name)
        self.assertEqual(datetime(2026, 5, 26, 8, 0, tzinfo=UTC), repository.nominal_trips[0].nom_start_time)

        # Realtime fields must remain untouched.
        self.assertEqual(datetime(2026, 5, 26, 8, 3, tzinfo=UTC), repository.nominal_trips[0].act_start_time)
        self.assertEqual("SCHEDULED", repository.nominal_trips[0].schedule_relationship)

        # Stop time nom fields must have been updated; act fields preserved.
        self.assertEqual(realtime_arrival, repository.nominal_stop_times[0].act_arrival_time)
        self.assertEqual("SCHEDULED", repository.nominal_stop_times[0].schedule_relationship)

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
            route_name="Route 1",
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
        self.assertIsNone(repository.realtime_trips[0].act_end_time)

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
            route_name="Route 1",
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
        self.assertEqual(last_nom_departure + timedelta(seconds=-300), repository.realtime_trips[0].act_end_time)

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
            route_name="Route 1",
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
            route_name="Route 1",
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
        self.assertEqual(last_nom_departure + timedelta(minutes=2), repository.realtime_trips[0].act_end_time)

    async def test_non_added_trip_without_nominal_data_is_discarded(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="unknown-trip",
            route_id="route-1",
            route_name="Route 1",
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
            route_name="route-X",
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
        # Alternative matching maps route-1 + "08:10" → "nominal-T1".
        repository.alternative_trip_id_lookup[("route-1", op_day, "08:10:00")] = "nominal-T1"

        # Realtime feed uses a different trip_id "feed-T1" that has no nominal entry.
        trip = TripRecord(
            operation_day_date=op_day,
            trip_id="feed-T1",
            route_id="route-1",
            route_name="route-1",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            scheduled_start_time_str="08:10:00",
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
            route_name="route-2",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            scheduled_start_time_str="09:00:00",
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

    async def test_non_added_trip_discarded_when_no_scheduled_start_time_str(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        now = datetime.now(UTC)
        trip = TripRecord(
            operation_day_date=now.date(),
            trip_id="feed-T3",
            route_id="route-3",
            route_name="route-3",
            operator_id=None,
            operator_name=None,
            schedule_relationship="SCHEDULED",
            scheduled_start_time_str=None,  # No start time → alternative matching impossible.
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


if __name__ == "__main__":
    unittest.main()

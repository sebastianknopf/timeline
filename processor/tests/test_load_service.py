from __future__ import annotations

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

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.upsert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.upsert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

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
        repository.nominal_stop_times = stop_times

        await service.load_nominal_trip_with_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(
            [
                ("upsert_nominal_trips", "demo", 1),
                ("upsert_nominal_stop_times", "demo", 1),
            ],
            repository.calls,
        )

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
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=first_nom_departure,
            nom_end_time=last_nom_arrival,
            act_start_time=first_act_departure,
            act_end_time=last_act_arrival,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
            act_total_distance=16.1,
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
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id="op-1",
            operator_name="Operator 1",
            nom_start_time=first_nom_departure,
            nom_end_time=last_nom_arrival,
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=10.0,
            act_total_distance=None,
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
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id=None,
            operator_name=None,
            nom_start_time=first_nom_departure,
            nom_end_time=last_nom_departure,
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=12.0,
            act_total_distance=None,
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
            concessionaire_id="conc-1",
            concessionaire_name="Concessionaire 1",
            operator_id=None,
            operator_name=None,
            nom_start_time=first_nom_departure,
            nom_end_time=last_nom_departure,
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=8.0,
            act_total_distance=None,
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


if __name__ == "__main__":
    unittest.main()

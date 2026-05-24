from __future__ import annotations

from datetime import UTC, date, datetime
import unittest

from processor.loading.loading_service import LoadingService
from processor.loading.models import StopRecord, StopTimeRecord, TripRecord
from processor.repository.intf_timeline_repository import TimelineRepositoryInterface


class RecordingRepository(TimelineRepositoryInterface):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def insert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        self.calls.append(("insert_nominal_stops", instance_id, len(stops)))

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.calls.append(("insert_nominal_trip_with_stop_times", instance_id, len(stop_times)))

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        self.calls.append(("upsert_realtime_trip", instance_id, 1))

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.calls.append(("upsert_realtime_stop_times", instance_id, len(stop_times)))


class LoadingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_nominal_stops_are_delegated_to_repository(self) -> None:
        repository = RecordingRepository()
        service = LoadingService(repository=repository)

        stops = [
            StopRecord(stop_id="A", stop_name="Stop A", stop_lat=48.1, stop_lon=8.7),
            StopRecord(stop_id="B", stop_name="Stop B", stop_lat=48.2, stop_lon=8.8),
        ]

        await service.load_nominal_stops(instance_id="demo", stops=stops)

        self.assertEqual([("insert_nominal_stops", "demo", 2)], repository.calls)

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
            )
        ]

        await service.load_nominal_trip_with_stop_times(
            instance_id="demo",
            trip=trip,
            stop_times=stop_times,
        )

        self.assertEqual(
            [("insert_nominal_trip_with_stop_times", "demo", 1)],
            repository.calls,
        )

    async def test_realtime_trip_and_stop_times_are_delegated_to_repository(self) -> None:
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
            act_start_time=datetime(2026, 5, 24, 8, 2, tzinfo=UTC),
            act_end_time=datetime(2026, 5, 24, 9, 3, tzinfo=UTC),
            nom_start_stop_id="A",
            nom_end_stop_id="B",
            nom_total_distance=15.5,
            act_total_distance=16.1,
        )
        stop_times = [
            StopTimeRecord(
                operation_day_date=date(2026, 5, 24),
                trip_id="trip-1",
                stop_id="A",
                distance_from_start=0.0,
                nom_arrival_time=datetime(2026, 5, 24, 8, 0, tzinfo=UTC),
                nom_departure_time=datetime(2026, 5, 24, 8, 1, tzinfo=UTC),
                act_arrival_time=datetime(2026, 5, 24, 8, 2, tzinfo=UTC),
                act_departure_time=datetime(2026, 5, 24, 8, 3, tzinfo=UTC),
            ),
            StopTimeRecord(
                operation_day_date=date(2026, 5, 24),
                trip_id="trip-1",
                stop_id="B",
                distance_from_start=10.0,
                nom_arrival_time=datetime(2026, 5, 24, 8, 40, tzinfo=UTC),
                nom_departure_time=datetime(2026, 5, 24, 8, 42, tzinfo=UTC),
                act_arrival_time=datetime(2026, 5, 24, 8, 45, tzinfo=UTC),
                act_departure_time=datetime(2026, 5, 24, 8, 47, tzinfo=UTC),
            ),
        ]

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


if __name__ == "__main__":
    unittest.main()

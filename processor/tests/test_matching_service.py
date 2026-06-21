from __future__ import annotations

from datetime import UTC, date, datetime
import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.models import TripRecord
from processor.matching.matching_service import MatchingService


class RecordingRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date, str | None, datetime | None, datetime | None, str | None, str | None]] = []
        self.result: list[str] | None = None

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
        self.calls.append(
            (
                instance_id,
                operation_day_date,
                route_id,
                scheduled_start_time,
                scheduled_end_time,
                scheduled_start_stop_id,
                scheduled_end_stop_id,
            )
        )
        return self.result


class MatchingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_match_returns_none_when_scheduled_start_time_missing(self) -> None:
        repository = RecordingRepository()
        service = MatchingService(repository=repository)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 1),
            trip_id="rt-trip-1",
            route_id="route-1",
            operator_id=None,
            operator_name=None,
            _t_scheduled_start_time=None,
        )

        matched_trip_id = await service.match(instance_id="demo", trip=trip)

        self.assertIsNone(matched_trip_id)
        self.assertEqual([], repository.calls)

    async def test_match_uses_route_operation_day_and_scheduled_start_time_lookup(self) -> None:
        repository = RecordingRepository()
        repository.result = ["nom-trip-42"]
        service = MatchingService(repository=repository)

        scheduled_start = datetime(2026, 6, 2, 1, 10, tzinfo=UTC)
        scheduled_end = datetime(2026, 6, 2, 2, 5, tzinfo=UTC)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 1),
            trip_id="rt-trip-2",
            route_id="route-2",
            operator_id=None,
            operator_name=None,
            _t_scheduled_start_time=scheduled_start,
            _t_scheduled_end_time=scheduled_end,
            _t_scheduled_start_stop_id="STOP-A",
            _t_scheduled_end_stop_id="STOP-B",
        )

        matched_trip_id = await service.match(instance_id="demo", trip=trip)

        self.assertEqual("nom-trip-42", matched_trip_id)
        self.assertEqual(
            [
                (
                    "demo",
                    date(2026, 6, 1),
                    "route-2",
                    scheduled_start,
                    scheduled_end,
                    "STOP-A",
                    "STOP-B",
                )
            ],
            repository.calls,
        )

    async def test_match_returns_none_when_repository_returns_no_matches(self) -> None:
        repository = RecordingRepository()
        repository.result = None
        service = MatchingService(repository=repository)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 1),
            trip_id="rt-trip-3",
            route_id="route-3",
            operator_id=None,
            operator_name=None,
            _t_scheduled_start_time=datetime(2026, 6, 1, 8, 0, tzinfo=UTC),
            _t_scheduled_end_time=datetime(2026, 6, 1, 9, 0, tzinfo=UTC),
            _t_scheduled_start_stop_id="STOP-S",
            _t_scheduled_end_stop_id="STOP-E",
        )

        matched_trip_id = await service.match(instance_id="demo", trip=trip)

        self.assertIsNone(matched_trip_id)

    async def test_match_returns_none_when_repository_returns_ambiguous_matches(self) -> None:
        repository = RecordingRepository()
        repository.result = ["nom-trip-1", "nom-trip-2"]
        service = MatchingService(repository=repository)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 1),
            trip_id="rt-trip-4",
            route_id="route-4",
            operator_id=None,
            operator_name=None,
            _t_scheduled_start_time=datetime(2026, 6, 1, 8, 30, tzinfo=UTC),
            _t_scheduled_end_time=datetime(2026, 6, 1, 9, 30, tzinfo=UTC),
            _t_scheduled_start_stop_id="STOP-S",
            _t_scheduled_end_stop_id="STOP-E",
        )

        matched_trip_id = await service.match(instance_id="demo", trip=trip)

        self.assertIsNone(matched_trip_id)


if __name__ == "__main__":
    unittest.main()

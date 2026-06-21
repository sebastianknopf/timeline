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
        self.calls: list[tuple[str, date, str, datetime]] = []
        self.result: str | None = None

    async def find_nominal_trip_id_by_properties(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str,
        scheduled_start_time: datetime,
    ) -> str | None:
        self.calls.append((instance_id, operation_day_date, route_id, scheduled_start_time))
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
        repository.result = "nom-trip-42"
        service = MatchingService(repository=repository)

        scheduled_start = datetime(2026, 6, 2, 1, 10, tzinfo=UTC)

        trip = TripRecord(
            operation_day_date=date(2026, 6, 1),
            trip_id="rt-trip-2",
            route_id="route-2",
            operator_id=None,
            operator_name=None,
            _t_scheduled_start_time=scheduled_start,
        )

        matched_trip_id = await service.match(instance_id="demo", trip=trip)

        self.assertEqual("nom-trip-42", matched_trip_id)
        self.assertEqual(
            [("demo", date(2026, 6, 1), "route-2", scheduled_start)],
            repository.calls,
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

import structlog

from ..repository.intf_timeline_repository import TimelineRepositoryInterface
from ..loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord

LOGGER = structlog.get_logger(__name__)


class MatchingService:
    """Matches a realtime trip against a set of nominal trips and returns the corresponding nominal trip ID - If no match is found, None will be returned."""

    def __init__(self, repository: TimelineRepositoryInterface) -> None:
        self._repository = repository

    async def match(self, instance_id: str, trip: TripRecord) -> str | None:
        """Attempt to resolve a nominal trip_id using route_id and scheduled start time.

        This secondary lookup is used when the primary trip_id from the realtime feed does not
        match any nominal entry for the operation day.  Returns the matched nominal trip_id, or
        None when no unambiguous match can be found.
        """
        if trip.scheduled_start_time_str is None:
            return None
        
        return await self._repository.find_nominal_trip_id_by_properties(
            instance_id=instance_id,
            operation_day_date=trip.operation_day_date,
            route_id=trip.route_id,
            scheduled_start_time_str=trip.scheduled_start_time_str,
        )

    
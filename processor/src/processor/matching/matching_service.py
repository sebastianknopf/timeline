from __future__ import annotations

import structlog

from ..common.global_id import GlobalId
from ..repository.intf_timeline_repository import TimelineRepositoryInterface
from ..loading.models import TripRecord

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
        if trip._t_scheduled_start_time is None:
            return None
        
        trip_ids: list[str] | None = await self._repository.find_nominal_trip_id_by_properties(
            instance_id=instance_id,
            operation_day_date=trip.operation_day_date,
            route_id=trip.route_id,
            scheduled_start_time=trip._t_scheduled_start_time,
            scheduled_end_time=trip._t_scheduled_end_time,
            scheduled_start_stop_id=GlobalId.level(
                trip._t_scheduled_start_stop_id, 
                3
            ),
            scheduled_end_stop_id=GlobalId.level(
                trip._t_scheduled_end_stop_id, 
                3
            )
        )

        if trip_ids is None:
            LOGGER.debug(
                    "matching_service_no_match_found",
                    instance_id=instance_id,
                    operation_day_date=trip.operation_day_date,
                    route_id=trip.route_id,
                    scheduled_start_time=trip._t_scheduled_start_time,
                    scheduled_end_time=trip._t_scheduled_end_time,
                    scheduled_start_stop_id=GlobalId.level(
                        trip._t_scheduled_start_stop_id, 
                        3
                    ),
                    scheduled_end_stop_id=GlobalId.level(
                        trip._t_scheduled_end_stop_id, 
                        3
                    )
                )
            
            return None

        if len(trip_ids) > 1:
            LOGGER.warning(
                    "matching_service_unambiguous_matches_found",
                    instance_id=instance_id,
                    operation_day_date=trip.operation_day_date,
                    route_id=trip.route_id,
                    scheduled_start_time=trip._t_scheduled_start_time,
                    scheduled_end_time=trip._t_scheduled_end_time,
                    scheduled_start_stop_id=GlobalId.level(
                        trip._t_scheduled_start_stop_id, 
                        3
                    ),
                    scheduled_end_stop_id=GlobalId.level(
                        trip._t_scheduled_end_stop_id, 
                        3
                    ),
                    trip_ids=trip_ids
                )

            return None

        return trip_ids[0] if trip_ids else None
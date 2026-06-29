from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import IntEnum

from processor.common.quality_issues import QualityIssue
import structlog

from ..repository.intf_timeline_repository import TimelineRepositoryInterface
from ..matching.matching_service import MatchingService
from .models import QualityIssueRecord, RequestRecord, RouteRecord, StopRecord, StopTimeRecord, TripRecord

LOGGER = structlog.get_logger(__name__)


class RealtimeLoadingResult(IntEnum):
    """Indicates the outcome of a realtime trip load attempt."""

    SUCCESS_DIRECT = 0
    SUCCESS_MATCHED = 1
    NO_NOMINAL_TRIP_FOUND = 2
    NO_AMBIGUOUS_NOMINAL_TRIP_FOUND = 3
    INTERNAL_ERROR = 4


@dataclass(frozen=True, slots=True)
class RealtimeLoadingQualityIssue:
    issue_type: QualityIssue
    assessment_value: str | None = None


class LoadingService:
    """Orchestrates loading while delegating persistence to repository interfaces."""

    def __init__(self, repository: TimelineRepositoryInterface) -> None:
        self._repository = repository

    async def load_nominal_stops_batch(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self._repository.upsert_nominal_stops(instance_id=instance_id, stops=stops)

    async def load_nominal_routes_batch(self, instance_id: str, routes: list[RouteRecord]) -> None:
        await self._repository.insert_nominal_routes(instance_id=instance_id, routes=routes)

    async def load_nominal_trips_batch(self, instance_id: str, trips: list[TripRecord]) -> None:
        await self._repository.insert_nominal_trips(instance_id=instance_id, trips=trips)

    async def load_nominal_stop_times_batch(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self._repository.insert_nominal_stop_times(
            instance_id=instance_id,
            stop_times=stop_times,
        )

    async def load_realtime_trip_and_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
        issue_handler: Callable[[RealtimeLoadingQualityIssue], None] | None = None,
    ) -> RealtimeLoadingResult:
        if not stop_times:
            return RealtimeLoadingResult.INTERNAL_ERROR

        # 1. step: Try getting nominal stop times by the trip ID.
        nominal_stop_times = await self._repository.get_nominal_stop_times_for_trip(
            instance_id=instance_id,
            operation_day_date=trip.operation_day_date,
            trip_id=trip.trip_id,
        )

        realtime_assignment_method: str | None = None
        
        # 2. step: If no nominal stop times were found, try to match the trip to a nominal trip using MatchingService.
        if nominal_stop_times:
            realtime_assignment_method = "DIRECT"    
        else:
            realtime_assignment_method = "MATCHING"
            
            # This is a trip which cannot be matched by ID directly. MappingService is up to find
            # the trip in multiple steps. See documentation of MatchingService for more information.            
            matching_service: MatchingService = MatchingService(self._repository)
            matched_trip_id: str | None = await matching_service.match(
                instance_id=instance_id,
                trip=trip,
            )

            if matched_trip_id is None:
                LOGGER.debug(
                    "realtime_trip_discarded_no_nominal_match",
                    instance_id=instance_id,
                    trip_id=trip.trip_id,
                    operation_day_date=str(trip.operation_day_date),
                )

                if issue_handler is not None:
                    issue_handler(
                        RealtimeLoadingQualityIssue(
                            issue_type=QualityIssue.NoNominalTripFound,
                            assessment_value=f"trip_id={trip.trip_id}, route_id={trip.route_id}, realtime_start_stop_id={trip._t_scheduled_start_stop_id}, realtime_end_stop_id={trip._t_scheduled_end_stop_id}, realtime_start_time={trip._t_scheduled_start_time.isoformat() if trip._t_scheduled_start_time is not None else None}, realtime_end_time={trip._t_scheduled_end_time.isoformat() if trip._t_scheduled_end_time is not None else None}"
                        )
                    )

                return RealtimeLoadingResult.NO_NOMINAL_TRIP_FOUND
            
            nominal_stop_times = await self._repository.get_nominal_stop_times_for_trip(
                instance_id=instance_id,
                operation_day_date=trip.operation_day_date,
                trip_id=matched_trip_id,
            )

            if not nominal_stop_times:
                LOGGER.debug(
                    "realtime_trip_discarded_no_nominal_stop_times",
                    instance_id=instance_id,
                    trip_id=trip.trip_id,
                    matched_trip_id=matched_trip_id,
                    operation_day_date=str(trip.operation_day_date),
                )

                return RealtimeLoadingResult.INTERNAL_ERROR 
            
            # Remap to the nominal trip_id for consistent DB primary-key usage.
            trip = replace(trip, trip_id=matched_trip_id)
            stop_times = [replace(st, trip_id=matched_trip_id) for st in stop_times]

        # 3. step: Apply nominal baseline to the realtime stop times, propagating delays forward where necessary.
        # this is the stage where realtime and nominal stop sequences are compared and merged
        normalized_input = self._apply_nominal_baseline(
            stop_times=stop_times, 
            nominal_stop_times=nominal_stop_times,
            issue_handler=issue_handler,
            realtime_is_complete_stop_sequence=trip._t_is_complete_stop_sequence
        )

        if not normalized_input:
            LOGGER.debug(
                "realtime_trip_discarded_empty_baseline_merge",
                instance_id=instance_id,
                trip_id=trip.trip_id,
                operation_day_date=str(trip.operation_day_date),
                feed_stop_count=len(stop_times),
                nominal_stop_count=len(nominal_stop_times)
            )

            return RealtimeLoadingResult.INTERNAL_ERROR

        # 4. step: Normalize the realtime stop times to resolve absolute timestamps and delay seconds into a single canonical representation.
        normalized_stop_times = self._normalize_realtime_stop_times(normalized_input)
        if not normalized_stop_times:
            return RealtimeLoadingResult.INTERNAL_ERROR

        explicit_count = sum(
            1 for s in normalized_input
            if any(
                s.stop_sequence == rt.stop_sequence for rt in stop_times
            )
        )
        propagated_count = len(normalized_input) - explicit_count

        LOGGER.debug(
            "realtime_trip_stop_times_resolved",
            instance_id=instance_id,
            trip_id=trip.trip_id,
            operation_day_date=str(trip.operation_day_date),
            nominal_stop_count=len(nominal_stop_times),
            feed_stop_count=len(stop_times),
            explicit_stop_count=explicit_count,
            propagated_stop_count=propagated_count,
            normalized_stop_count=len(normalized_stop_times),
        )

        # 5. step: Derive trip-level fields from the normalized stop times and the nominal trip.
        nominal_trip = await self._repository.get_nominal_trip(
            instance_id=instance_id,
            operation_day_date=trip.operation_day_date,
            trip_id=trip.trip_id,
        )

        normalized_trip = self._derive_realtime_trip_fields(
            source_trip=trip,
            nominal_stop_times=nominal_stop_times,
            normalized_stop_times=normalized_stop_times,
            nominal_trip=nominal_trip,
        )

        normalized_trip = replace(
            normalized_trip,
            realtime_assignment_method=realtime_assignment_method,
        )

        # Matching strategy hooks belong here while DB writes remain in the repository.
        await self._repository.upsert_realtime_trip(instance_id=instance_id, trip=normalized_trip)
        await self._repository.upsert_realtime_stop_times(
            instance_id=instance_id,
            stop_times=normalized_stop_times,
        )

        if realtime_assignment_method == "DIRECT":
            return RealtimeLoadingResult.SUCCESS_DIRECT
        else:
            return RealtimeLoadingResult.SUCCESS_MATCHED

    async def load_request(
        self,
        instance_id: str,
        request: RequestRecord,
    ) -> None:
        await self._repository.insert_request(
            instance_id=instance_id, 
            request=request
        )

    async def load_quality_issues_batch(
        self,
        instance_id: str,
        quality_issues: list[QualityIssueRecord],
    ) -> None:
        await self._repository.upsert_quality_issues(
            instance_id=instance_id,
            quality_issues=quality_issues,
        )   

    def _apply_nominal_baseline(
        self,
        stop_times: list[StopTimeRecord],
        nominal_stop_times: list[StopTimeRecord],
        issue_handler: Callable[[RealtimeLoadingQualityIssue], None] | None = None,
        realtime_is_complete_stop_sequence: bool = False
    ) -> list[StopTimeRecord]:
        """Apply the nominal baseline to the realtime stop times, propagating delays forward where necessary.
        This method also checks for unexpected and missing stops in the realtime feed compared to the nominal baseline.
        If an issue_handler is provided, it will be called with any detected quality issues.
        
        Args:
            stop_times (list[StopTimeRecord]): The list of realtime stop times to be processed
            nominal_stop_times (list[StopTimeRecord]): The list of nominal stop times to be used as a baseline
            issue_handler (Callable[[RealtimeLoadingQualityIssue], None], optional): A callable that will be called with any detected quality issues. Defaults to None.
            realtime_is_complete_stop_sequence (bool, optional): A flag indicating whether the realtime feed is expected to be a complete stop sequence. Defaults to False.

        Returns:
            list[StopTimeRecord]: The list of stop times after applying the nominal baseline.
        """

        # Defensive guard: all trips reaching this point must have nominal data since
        # non-nominal trips are discarded earlier in load_realtime_trip_and_stop_times.
        if not nominal_stop_times:
            return stop_times

        if issue_handler is not None:
            # if the issue handler is set, check for unexpected and missing stops in the realtime feed compared to the nominal baseline

            # check for unexpected stops ...
            unexpected_realtime_stops = [
                rt
                for rt in stop_times
                if (
                    rt.schedule_relationship != "ADDED"
                    and not any(
                        ns.stop_id == rt.stop_id
                        for ns in nominal_stop_times
                    )
                )
            ]

            for unexpected_stop in unexpected_realtime_stops:
                issue_handler(RealtimeLoadingQualityIssue(issue_type=QualityIssue.UnexpectedStopFound, assessment_value=unexpected_stop.stop_id))
            
            # check for missing stops ...
            # if the realtime feed is expected to be a complete stop sequence
            if realtime_is_complete_stop_sequence:
                missing_realtime_stops = [
                    ns
                    for ns in nominal_stop_times
                    if not any(
                        rt.stop_sequence == ns.stop_sequence
                        and rt.stop_id == ns.stop_id
                        for rt in stop_times
                    )
                ]

                for missing_stop in missing_realtime_stops:
                    issue_handler(RealtimeLoadingQualityIssue(issue_type=QualityIssue.ExpectedStopMissing, assessment_value=missing_stop.stop_id))

        # apply the nominal baseline to the realtime stop times, propagating delays forward where necessary
        realtime_by_sequence: dict[int, StopTimeRecord] = {item.stop_sequence: item for item in stop_times}
        ordered_nominal = sorted(nominal_stop_times, key=lambda s: s.stop_sequence)

        merged: list[StopTimeRecord] = []
        last_arrival_delay_s: int | None = None
        last_departure_delay_s: int | None = None

        for baseline in ordered_nominal:
            record = realtime_by_sequence.get(baseline.stop_sequence)

            if record is not None:
                # Explicit realtime update: apply nominal baseline values.
                merged.append(
                    replace(
                        record,
                        nom_arrival_time=baseline.nom_arrival_time,
                        nom_departure_time=baseline.nom_departure_time,
                        distance_from_start=baseline.distance_from_start,
                    )
                )
                # Update tracked delay for forward propagation to subsequent stops.
                # Absolute time takes priority; delay_seconds is used as a fallback.
                if record.act_arrival_time is not None:
                    last_arrival_delay_s = round(
                        (record.act_arrival_time - baseline.nom_arrival_time).total_seconds()
                    )
                elif record.arrival_delay_seconds is not None:
                    last_arrival_delay_s = record.arrival_delay_seconds

                if record.act_departure_time is not None:
                    last_departure_delay_s = round(
                        (record.act_departure_time - baseline.nom_departure_time).total_seconds()
                    )
                elif record.departure_delay_seconds is not None:
                    last_departure_delay_s = record.departure_delay_seconds

            elif last_arrival_delay_s is not None or last_departure_delay_s is not None:
                # No explicit update for this stop, but a preceding update provides a
                # propagation basis.  Synthesize a stop-time record by applying the
                # tracked delay to the nominal baseline.  Schedule relationship is always
                # SCHEDULED for propagated stops; an explicit update for the same stop
                # in a later position in this feed would override this value.
                merged.append(
                    StopTimeRecord(
                        operation_day_date=baseline.operation_day_date,
                        trip_id=baseline.trip_id,
                        stop_id=baseline.stop_id,
                        distance_from_start=baseline.distance_from_start,
                        nom_arrival_time=baseline.nom_arrival_time,
                        nom_departure_time=baseline.nom_departure_time,
                        act_arrival_time=None,
                        act_departure_time=None,
                        schedule_relationship="SCHEDULED",
                        stop_sequence=baseline.stop_sequence,
                        arrival_delay_seconds=last_arrival_delay_s,
                        departure_delay_seconds=last_departure_delay_s,
                    )
                )

        explicit_count = sum(1 for b in ordered_nominal if realtime_by_sequence.get(b.stop_sequence) is not None)
        propagated_count = len(merged) - explicit_count
        skipped_count = len(ordered_nominal) - len(merged)

        LOGGER.debug(
            "realtime_nominal_baseline_applied",
            explicit_count=explicit_count,
            propagated_count=propagated_count,
            skipped_count=skipped_count,
            merged_count=len(merged),
        )

        return merged

    def _normalize_realtime_stop_times(self, stop_times: list[StopTimeRecord]) -> list[StopTimeRecord]:
        ordered = sorted(stop_times, key=_stop_time_order_key)

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

            # Keep arrival/departure paired for one stop when only one realtime side exists.
            if resolved_act_arrival is None and resolved_act_departure is not None:
                resolved_act_arrival = resolved_act_departure
            if resolved_act_departure is None and resolved_act_arrival is not None:
                resolved_act_departure = resolved_act_arrival

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
        nominal_stop_times: list[StopTimeRecord],
        normalized_stop_times: list[StopTimeRecord],
        nominal_trip: TripRecord | None = None,
    ) -> TripRecord:
        ordered_nominal = (
            sorted(nominal_stop_times, key=_stop_time_order_key)
            if nominal_stop_times
            else sorted(normalized_stop_times, key=lambda item: (item.stop_sequence, item.distance_from_start, item.stop_id))
        )

        first_stop = ordered_nominal[0]
        last_stop = ordered_nominal[-1]

        nom_start_time = first_stop.nom_departure_time
        nom_end_time = last_stop.nom_departure_time

        # act_start_time must always reflect the first NOMINAL stop regardless of how many
        # leading stops are missing from the realtime feed.  Look up whether the first
        # nominal stop has an entry in normalized_stop_times (it may not when the feed
        # only covers a subset of stops and no prior delay has been propagated forward).
        # If the first nominal stop is present and carries act_departure_time, use that;
        # otherwise fall back to nom_start_time so that the trip boundary is never driven
        # by a stop somewhere in the middle of the trip.
        rt_first_nominal = next(
            (s for s in normalized_stop_times if s.stop_sequence == first_stop.stop_sequence),
            None,
        )

        act_start_time = (
            rt_first_nominal.act_departure_time
            if rt_first_nominal is not None and rt_first_nominal.act_departure_time is not None
            else nom_start_time
        )

        # act_end_time uses the last entry in normalized_stop_times (earliest/latest
        # available realtime coverage), which is correct for the end boundary.
        rt_last = normalized_stop_times[-1]

        if rt_last.act_arrival_time is not None:
            act_end_time = rt_last.act_arrival_time
        elif rt_last.act_departure_time is not None:
            act_end_time = rt_last.act_departure_time
        else:
            act_end_time = nom_end_time

        nom_total_distance = max(item.distance_from_start for item in ordered_nominal)

        # Prefer the trip-level nom_total_distance stored by the nominal pipeline over the
        # stop-level max.  The nominal pipeline may have derived nom_total_distance from the
        # shape index when stop_times.shape_dist_traveled is absent, which correctly stores a
        # non-zero trip distance in dim_trips even when all per-stop distance_from_start values
        # are 0.0.  Without this fallback, act_total_distance would be 0.0 for SCHEDULED trips
        # in feeds that lack per-stop distance data.
        if nominal_trip is not None and (nominal_trip.nom_total_distance or 0.0) > 0.0:
            nom_total_distance = nominal_trip.nom_total_distance  # type: ignore[assignment]

        # Derive act_total_distance based on the trip's schedule_relationship.
        # SCHEDULED trips are assumed to have operated their full nominal distance, so
        # nom_total_distance is copied to act_total_distance.
        # All other relationships (UNKNOWN, CANCELLED, …) contribute zero actual kilometres
        # because the trip either did not run or its operational status is unknown.
        # NOTE: this is an all-or-nothing attribution model — a trip that was only
        # partially operated is treated identically to a fully operated one (SCHEDULED)
        # or a fully cancelled one. Per-segment partial-distance accounting is not
        # supported in this version.
        if source_trip.schedule_relationship == "SCHEDULED":
            act_total_distance: float = nom_total_distance
        else:
            act_total_distance = 0.0

        return replace(
            source_trip,
            nom_start_time=nom_start_time,
            nom_end_time=nom_end_time,
            act_start_time=act_start_time,
            act_end_time=act_end_time,
            nom_start_stop_id=first_stop.stop_id,
            nom_end_stop_id=last_stop.stop_id,
            nom_total_distance=nom_total_distance,
            act_total_distance=act_total_distance,
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


def _stop_time_order_key(record: StopTimeRecord) -> tuple[int, datetime, float, str]:
    return (
        record.stop_sequence,
        record.nom_departure_time,
        record.distance_from_start,
        record.stop_id,
    )

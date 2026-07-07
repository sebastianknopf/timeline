from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, time, datetime, timedelta
import fnmatch
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from google.transit import gtfs_realtime_pb2
import requests
import structlog

from ..common.global_id import GlobalId
from ..common.quality_issues import QualityIssue
from ..loading.loading_service import LoadingService, RealtimeLoadingResult, RealtimeLoadingQualityIssue
from ..loading.models import StopTimeRecord, TripRecord
from ..mapping.intf_mapping_service import MappingServiceInterface
from ..runtime_config import AuthenticationConfig, FilterEntryConfig, InstanceConfig, PipelineConfig
from .base_pipeline import RealtimePipelineBase

LOGGER = structlog.get_logger(__name__)


class HttpError(RuntimeError):
    """Raised when an HTTP request to a GTFS realtime endpoint fails."""

    def __init__(self, status_code: int, message: str | None = None) -> None:
        super().__init__(self.message)

        self.status_code = status_code
        self.message = message or f"HTTP request failed with status code {status_code}"


class GtfsRealtimePipelineError(RuntimeError):
    """Raised when GTFS realtime processing cannot produce a valid payload."""


class GtfsRtTripUpdatesPipeline(RealtimePipelineBase):
    def __init__(
        self,
        loading_service: LoadingService,
        mapping_service: MappingServiceInterface,
        processor_timezone_name: str = "UTC",
    ) -> None:
        super().__init__(loading_service=loading_service)

        self._loading_service = loading_service
        self._mapping_service = mapping_service
        self._processor_timezone_name = processor_timezone_name
        self._processor_timezone = _safe_zoneinfo(processor_timezone_name)

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        if pipeline.name != "gtfsrt-tripupdates":
            raise GtfsRealtimePipelineError(
                f"GtfsRtTripUpdatesPipeline cannot execute pipeline '{pipeline.id}' with name '{pipeline.name}'."
            )

        pipeline_timezone: ZoneInfo = _safe_zoneinfo(pipeline.timezone)

        try:
            payload = self._read_endpoint_payload(endpoint=pipeline.endpoint, authentication=pipeline.authentication)
            feed_message = self._decode_feed_message(payload)

            now_utc = datetime.now(UTC)
            now_processor_tz = now_utc.astimezone(self._processor_timezone)
            feed_timestamp_utc = _timestamp_to_utc(feed_message.header.timestamp) if feed_message.header.timestamp else None

            trip_update_count = 0
            loaded_direct_trip_count = 0
            loaded_matched_trip_count = 0
            loaded_stop_time_count = 0
            route_filter = pipeline.filter.routes if pipeline.filter is not None else ()

            for entity in feed_message.entity:
                if not entity.HasField("trip_update"):
                    continue

                trip_update = entity.trip_update
                trip_descriptor = trip_update.trip
                trip_id = (trip_descriptor.trip_id or "").strip()
                if not trip_id:
                    continue

                trip_update_count += 1

                operation_day = _parse_service_date(
                    raw_value=(trip_descriptor.start_date or "").strip(),
                    fallback_date=now_processor_tz.date(),
                )

                route_id = (trip_descriptor.route_id or "").strip() or "UNKNOWN-ROUTE"

                if route_filter:
                    if not trip_descriptor.route_id or not _route_matches_filter(route_id, route_filter):
                        LOGGER.debug(
                            "realtime_trip_discarded_by_route_filter",
                            instance_id=instance.id,
                            pipeline_id=pipeline.id,
                            trip_id=trip_id,
                            route_id=route_id,
                        )
                        continue

                scheduled_start_time: datetime | None = _parse_gtfs_time(
                    raw_value=(trip_descriptor.start_time or "").strip(),
                    operation_day=operation_day,
                    source_timezone=pipeline_timezone,
                    target_timezone=self._processor_timezone
                )

                trip_schedule_relationship = _enum_name(
                    enum_descriptor=gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship,
                    value=trip_descriptor.schedule_relationship,
                    fallback="UNKNOWN",
                )

                if trip_schedule_relationship == "ADDED":
                    LOGGER.debug(
                        "realtime_trip_discarded_added_schedule_relationship",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                        trip_id=trip_id,
                        route_id=route_id,
                        operation_day_date=str(operation_day),
                    )

                    continue
                
                stop_time_records = self._build_stop_time_records(
                    operation_day=operation_day,
                    trip_id=trip_id,
                    updates=trip_update.stop_time_update,
                    default_schedule_relationship=trip_schedule_relationship,
                    placeholder_nominal_time=now_processor_tz
                )

                trip_record = self._build_trip_record(
                    pipeline=pipeline,
                    operation_day=operation_day,
                    trip_id=trip_id,
                    route_id=route_id,
                    schedule_relationship=trip_schedule_relationship,
                    scheduled_start_time=scheduled_start_time,
                )

                # apply mapping
                mapped_trip, mapped_stop_times = await self._mapping_service.map_records_for_loading(
                    instance_id=instance.id,
                    pipeline_id=pipeline.id,
                    trip=trip_record,
                    stop_times=stop_time_records,
                )

                if not mapped_stop_times:
                    continue

                # run quality monitoring
                self._monitor_quality_issues(
                    instance=instance,
                    pipeline=pipeline,
                    now_processor_tz=now_processor_tz,
                    entity=entity,
                    trip_record=trip_record,
                    stop_time_records=stop_time_records
                )

                # finally load the entity into the database
                # issue handler is a callback that will be called for each quality issue detected during loading
                def loading_issue_handler(issue: RealtimeLoadingQualityIssue) -> None:
                    self.report_quality_issue(
                        instance=instance,
                        pipeline=pipeline,
                        timestamp=now_processor_tz,
                        entity_id=entity.id,
                        issue_type_id=issue.issue_type,
                        assessment_value=issue.assessment_value,
                    )

                realtime_loading_result: RealtimeLoadingResult = await self._loading_service.load_realtime_trip_and_stop_times(
                    instance_id=instance.id,
                    trip=mapped_trip,
                    stop_times=mapped_stop_times,
                    issue_handler=loading_issue_handler
                )

                if realtime_loading_result == RealtimeLoadingResult.SUCCESS_DIRECT:
                    loaded_direct_trip_count += 1
                elif realtime_loading_result == RealtimeLoadingResult.SUCCESS_MATCHED:
                    loaded_matched_trip_count += 1

            LOGGER.info(
                "gtfs_realtime_tripupdates_pipeline_loaded",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                processor_timezone=self._processor_timezone_name,
                num_entities=trip_update_count,
                loaded_direct_trip_count=loaded_direct_trip_count,
                loaded_matched_trip_count=loaded_matched_trip_count,
                loaded_stop_time_count=loaded_stop_time_count
            )

            self.report_request(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                num_entities=trip_update_count,
                loaded_direct_trip_count=loaded_direct_trip_count,
                loaded_matched_trip_count=loaded_matched_trip_count,
                age_seconds=(now_processor_tz - feed_timestamp_utc).total_seconds() if feed_timestamp_utc else 0,
                status_code=200
            )

        except HttpError as http_exc:
            LOGGER.exception(
                "gtfs_realtime_tripupdates_pipeline_http_error",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                processor_timezone=self._processor_timezone_name,
                status_code=http_exc.status_code,
                error=str(http_exc),
            )

            self.report_request(
                instance=instance,
                pipeline=pipeline,
                timestamp=datetime.now(self._processor_timezone),
                num_entities=0,
                age_seconds=0,
                status_code=http_exc.status_code
            )

        except Exception as exc:
            LOGGER.exception(
                "gtfs_realtime_tripupdates_pipeline_error",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                processor_timezone=self._processor_timezone_name,
                error=str(exc),
            )
            
            self.report_request(
                instance=instance,
                pipeline=pipeline,
                timestamp=datetime.now(self._processor_timezone),
                num_entities=0,
                age_seconds=0,
                status_code=0
            )

        finally:

            # submit data quality report independent of success or failure of the realtime processing
            await self.submit_quality_report(instance=instance)

    def _read_endpoint_payload(self, endpoint: str, authentication: AuthenticationConfig | None) -> bytes:
        """Read the GTFS-RT protobuf payload from the configured endpoint.

        Authentication headers are generated by the shared base-pipeline helper
        to keep auth behavior consistent across pipeline implementations.
        """
        headers = self._build_auth_headers(authentication)

        cert_filename: str | None = authentication.cert if authentication is not None else None
        key_filename: str | None = authentication.key if authentication is not None else None

        with requests.get(
            endpoint, 
            headers=headers, 
            timeout=30,
            cert=(cert_filename, key_filename) if cert_filename and key_filename else None
        ) as response:
            status_code = response.status_code
            if status_code != 200:
                raise HttpError(status_code, f"HTTP request to {endpoint} failed with status code {status_code}")

            return response.content

    def _decode_feed_message(self, payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
        feed_message = gtfs_realtime_pb2.FeedMessage()
        try:
            feed_message.ParseFromString(payload)
        except Exception as exc:
            raise GtfsRealtimePipelineError("Failed to decode GTFS realtime protobuf payload.") from exc
        return feed_message

    def _monitor_quality_issues(
        self,
        instance: InstanceConfig,
        pipeline: PipelineConfig,
        now_processor_tz: datetime,
        entity: any,
        trip_record: TripRecord,
        stop_time_records: list[StopTimeRecord]
    ) -> None:
        if not GlobalId.is_global_id(trip_record.trip_id):
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.TripIdNonGlobal,
                assessment_value=trip_record.trip_id,
            )

        if not entity.HasField("trip_update") or not entity.trip_update.trip.HasField("start_date"):
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.OperationDayIsNull,
            )

        if trip_record.route_id == "UNKNOWN-ROUTE":
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.RouteIdIsNull,
            )

        if trip_record.route_id != "UNKNOWN-ROUTE" and not GlobalId.is_global_id(trip_record.route_id):
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=entity.id,
                issue_type_id=QualityIssue.RouteIdNonGlobal,
                assessment_value=trip_record.route_id,
            )

        for stop_time_record in stop_time_records:
            if not GlobalId.is_global_id(stop_time_record.stop_id):
                self.report_quality_issue(
                    instance=instance,
                    pipeline=pipeline,
                    timestamp=now_processor_tz,
                    entity_id=entity.id,
                    issue_type_id=QualityIssue.StopIdNonGlobal,
                    assessment_value=stop_time_record.stop_id,
                )

            if (
                stop_time_record.act_arrival_time is not None
                and stop_time_record.act_departure_time is not None
                and stop_time_record.act_departure_time < stop_time_record.act_arrival_time
            ):
                self.report_quality_issue(
                    instance=instance,
                    pipeline=pipeline,
                    timestamp=now_processor_tz,
                    entity_id=entity.id,
                    issue_type_id=QualityIssue.EstimatedDepatureTimeBeforeArrivalTime,
                    assessment_value=(
                        f"{stop_time_record.act_departure_time.isoformat()} < "
                        f"{stop_time_record.act_arrival_time.isoformat()}"
                    ),
                )

    def _build_trip_record(
        self,
        pipeline: PipelineConfig,
        operation_day: date,
        trip_id: str,
        route_id: str,
        schedule_relationship: str,
        scheduled_start_time: datetime | None,
    ) -> TripRecord:
        # Only identity fields are set here.  All derived trip boundary fields
        # (nom/act start/end times, stop IDs, distances) are resolved from nominal
        # data by the loading service.  Concessionaire and operator fields are not
        # set because the realtime pipeline does not own them; the upsert only updates
        # act_* fields on conflict, so those fields are never written by the realtime path.
        return TripRecord(
            operation_day_date=operation_day,
            trip_id=trip_id,
            route_id=route_id,
            operator_id=None,
            operator_name=None,
            schedule_relationship=schedule_relationship,
            realtime_pipeline_id=pipeline.id,
            _t_scheduled_start_time=scheduled_start_time,
        )

    def _build_stop_time_records(
        self,
        operation_day: date,
        trip_id: str,
        updates: Iterable[gtfs_realtime_pb2.TripUpdate.StopTimeUpdate],
        default_schedule_relationship: str,
        placeholder_nominal_time: datetime,
    ) -> list[StopTimeRecord]:
        records: list[StopTimeRecord] = []

        for index, update in enumerate(updates):
            stop_id = (update.stop_id or "").strip()
            if not stop_id:
                continue

            stop_sequence = update.stop_sequence if update.HasField("stop_sequence") else index + 1

            actual_arrival_time = (
                _resolve_event_time(
                    event=update.arrival,
                    processor_timezone=self._processor_timezone,
                )
                if update.HasField("arrival")
                else None
            )

            actual_departure_time = (
                _resolve_event_time(
                    event=update.departure,
                    processor_timezone=self._processor_timezone,
                )
                if update.HasField("departure")
                else None
            )

            has_arrival_delay = update.HasField("arrival") and update.arrival.HasField("delay")
            has_departure_delay = update.HasField("departure") and update.departure.HasField("delay")

            if actual_arrival_time is None and actual_departure_time is None and not has_arrival_delay and not has_departure_delay:
                continue

            schedule_relationship = (
                _enum_name(
                    enum_descriptor=gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship,
                    value=update.schedule_relationship,
                    fallback=default_schedule_relationship,
                )
                if update.HasField("schedule_relationship")
                else default_schedule_relationship
            )

            distance_from_start = 0.0

            # Use event timestamps as nominal placeholders so delay-based computations are
            # anchored to actual event times rather than the current wall-clock time.
            # LoadingService._apply_nominal_baseline replaces these with the real scheduled
            # times from the nominal baseline before any DB write (when nominal is loaded).
            nom_arrival_time = actual_arrival_time or actual_departure_time or placeholder_nominal_time
            nom_departure_time = actual_departure_time or actual_arrival_time or placeholder_nominal_time

            records.append(
                StopTimeRecord(
                    operation_day_date=operation_day,
                    trip_id=trip_id,
                    stop_id=stop_id,
                    distance_from_start=distance_from_start,
                    nom_arrival_time=nom_arrival_time,
                    nom_departure_time=nom_departure_time,
                    act_arrival_time=actual_arrival_time,
                    act_departure_time=actual_departure_time,
                    schedule_relationship=schedule_relationship,
                    stop_sequence=stop_sequence,
                    arrival_delay_seconds=update.arrival.delay if update.HasField("arrival") and update.arrival.HasField("delay") else None,
                    departure_delay_seconds=update.departure.delay if update.HasField("departure") and update.departure.HasField("delay") else None,
                )
            )

        return records


def _route_matches_filter(route_id: str, route_filter: tuple[FilterEntryConfig, ...]) -> bool:
    include_rules = [rule for rule in route_filter if rule.type == "include"]
    exclude_rules = [rule for rule in route_filter if rule.type == "exclude"]

    if include_rules and not any(fnmatch.fnmatchcase(route_id, rule.match) for rule in include_rules):
        return False

    if any(fnmatch.fnmatchcase(route_id, rule.match) for rule in exclude_rules):
        return False

    return True

def _safe_zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        LOGGER.warning("gtfsrt_invalid_timezone_fallback", timezone=timezone_name)
        return ZoneInfo("UTC")

def _timestamp_to_utc(unix_seconds: int) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=UTC)

def _resolve_event_time(
    event: gtfs_realtime_pb2.TripUpdate.StopTimeEvent,
    processor_timezone: ZoneInfo,
) -> datetime | None:
    # Absolute event timestamp is authoritative; delay is resolved in the load service against nominal time.
    if event.HasField("time"):
        return _timestamp_to_utc(event.time).astimezone(processor_timezone)

    return None

def _parse_service_date(raw_value: str, fallback_date: date) -> date:
    if not raw_value:
        return fallback_date

    try:
        return datetime.strptime(raw_value, "%Y%m%d").date()
    except ValueError:
        return fallback_date
    
def _parse_gtfs_time(
    raw_value: str,
    operation_day: date,
    source_timezone: ZoneInfo,
    target_timezone: ZoneInfo,
) -> datetime | None:
    if not raw_value:
        return None

    parts = raw_value.split(":")
    if len(parts) != 3:
        return None

    hours = _parse_int(parts[0])
    minutes = _parse_int(parts[1])
    seconds = _parse_int(parts[2])
    if hours is None or minutes is None or seconds is None:
        return None
    if minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60 or hours < 0:
        return None

    base = datetime.combine(operation_day, time(0, 0), tzinfo=source_timezone)
    source_timestamp = base + timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return source_timestamp.astimezone(target_timezone)


def _parse_int(raw_value: str) -> int | None:
    try:
        return int(raw_value)
    except ValueError:
        return None


def _enum_name(enum_descriptor: object, value: int, fallback: str) -> str:
    if not hasattr(enum_descriptor, "Name"):
        return fallback
    try:
        name = enum_descriptor.Name(value)
    except ValueError:
        return fallback
    if not name:
        return fallback
    return str(name)

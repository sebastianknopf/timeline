from __future__ import annotations

import fnmatch
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime, time, timedelta
from typing import Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
import structlog

from ..common.global_id import GlobalId
from ..common.quality_issues import QualityIssue
from ..loading.loading_service import LoadingService, RealtimeLoadingQualityIssue, RealtimeLoadingResult
from ..loading.models import StopTimeRecord, TripRecord
from ..mapping.intf_mapping_service import MappingServiceInterface
from ..runtime_config import FilterEntryConfig, InstanceConfig, PipelineConfig
from .base_pipeline import RealtimePipelineBase

LOGGER = structlog.get_logger(__name__)

_SIRI_NAMESPACE = "http://www.siri.org.uk/siri"
_DEFAULT_OPERATION_DAY_END = time(3, 0)


class SiriEtLightPipelineError(RuntimeError):
    """Raised when SIRI-ET Light processing cannot produce a valid payload."""


class HttpError(RuntimeError):
    """Raised when an HTTP request to a SIRI-ET endpoint fails."""

    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        self.message = message or f"HTTP request failed with status code {status_code}"
        super().__init__(self.message)


class SiriEtLightPipeline(RealtimePipelineBase):
    """Realtime pipeline for SIRI Estimated Timetable (Light) feeds."""

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
        dpl_appendix_pattern: str | None = (
            str(pipeline.parameters["dpl_appendix_pattern"])
            if "dpl_appendix_pattern" in pipeline.parameters
            else None
        )

        operation_day_end: time = _parse_time_of_day(
            str(pipeline.parameters.get("operation_day_end", "03:00"))
        )

        route_filter = pipeline.filter.routes if pipeline.filter is not None else ()
        operator_filter = pipeline.filter.operators if pipeline.filter is not None else ()

        try:
            if pipeline.name != "siri-et-light":
                raise SiriEtLightPipelineError(
                    f"SiriEtLightPipeline cannot execute pipeline '{pipeline.id}' with name '{pipeline.name}'."
                )

            payload = self._read_endpoint_payload(
                endpoint=pipeline.endpoint,
                authentication=pipeline.authentication,
            )

            now_utc = datetime.now(UTC)
            now_processor_tz = now_utc.astimezone(self._processor_timezone)

            root = self._parse_xml(payload)
            ns = _detect_namespace(root)

            journey_count = 0
            loaded_direct_trip_count = 0
            loaded_matched_trip_count = 0

            for journey in _iter_estimated_vehicle_journeys(root, ns):

                # --- Extra trip: discard (A17 / T8) ---
                extra_trip_text = _find_text(journey, ns, "ExtraTrip")
                if extra_trip_text is not None and extra_trip_text.lower() == "true":
                    LOGGER.debug(
                        "siri_et_light_trip_discarded_extra_trip",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                    )
                    continue

                # --- Extract key identity fields ---
                line_ref = _strip_appendix(
                    _find_text(journey, ns, "LineRef") or "", dpl_appendix_pattern
                ).strip()

                operator_ref = _strip_appendix(
                    _find_text(journey, ns, "OperatorRef") or "", dpl_appendix_pattern
                ).strip() or None

                dated_vehicle_journey_ref = _strip_appendix(
                    _find_text(journey, ns, "DatedVehicleJourneyRef") or "", dpl_appendix_pattern
                ).strip()

                vehicle_journey_ref = _strip_appendix(
                    _find_text(journey, ns, "VehicleJourneyRef") or "", dpl_appendix_pattern
                ).strip()

                # A5: prefer DatedVehicleJourneyRef, fallback to VehicleJourneyRef
                trip_id = dated_vehicle_journey_ref or vehicle_journey_ref
                if not trip_id:
                    LOGGER.debug(
                        "siri_et_light_trip_discarded_no_trip_id",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                    )

                    continue

                # A9: LineRef is the route ID
                route_id = line_ref or "UNKNOWN-ROUTE"

                # --- Route filter ---
                if route_filter:
                    if not line_ref or not _matches_filter(line_ref, route_filter):
                        LOGGER.debug(
                            "siri_et_light_trip_discarded_by_route_filter",
                            instance_id=instance.id,
                            pipeline_id=pipeline.id,
                            trip_id=trip_id,
                            route_id=route_id,
                        )

                        continue

                # --- Operator filter ---
                if operator_filter:
                    if not operator_ref or not _matches_filter(operator_ref, operator_filter):
                        LOGGER.debug(
                            "siri_et_light_trip_discarded_by_operator_filter",
                            instance_id=instance.id,
                            pipeline_id=pipeline.id,
                            trip_id=trip_id,
                            operator_ref=operator_ref,
                        )

                        continue

                # --- Schedule relationship (T6) ---
                cancellation_text = _find_text(journey, ns, "Cancellation")
                monitored_text = _find_text(journey, ns, "Monitored")

                is_cancelled: bool = cancellation_text is not None and cancellation_text.lower() == "true"
                is_monitored: bool | None = (
                    True if monitored_text is not None and monitored_text.lower() == "true"
                    else False if monitored_text is not None and monitored_text.lower() == "false"
                    else None
                )

                # A14: discard unmonitored trips
                if is_monitored is False and not is_cancelled:
                    LOGGER.debug(
                        "siri_et_light_trip_discarded_not_monitored",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                        trip_id=trip_id,
                    )
                    
                    continue

                if is_cancelled:
                    trip_schedule_relationship = "CANCELED"
                elif is_monitored is True:
                    trip_schedule_relationship = "SCHEDULED"
                else:
                    trip_schedule_relationship = "UNKNOWN"

                # --- Collect calls (A10: both EstimatedCall and RecordedCall) ---
                raw_calls = _collect_calls(journey, ns)

                if not raw_calls and not is_cancelled:
                    LOGGER.debug(
                        "siri_et_light_trip_discarded_no_calls",
                        instance_id=instance.id,
                        pipeline_id=pipeline.id,
                        trip_id=trip_id,
                    )

                    continue

                # --- Operation day (T2 / A2) ---
                operation_day = _derive_operation_day(
                    journey=journey,
                    ns=ns,
                    raw_calls=raw_calls,
                    fallback_date=now_processor_tz.date(),
                    operation_day_end=operation_day_end,
                    processor_timezone=self._processor_timezone,
                )

                # --- IsCompleteStopSequence (A15 / A16) ---
                is_complete_text = _find_text(journey, ns, "IsCompleteStopSequence")
                is_complete_stop_sequence: bool = (
                    is_complete_text is not None and is_complete_text.lower() == "true"
                )

                # --- Build stop time records ---
                stop_time_records = self._build_stop_time_records(
                    operation_day=operation_day,
                    trip_id=trip_id,
                    raw_calls=raw_calls,
                    trip_schedule_relationship=trip_schedule_relationship,
                    ns=ns,
                    dpl_appendix_pattern=dpl_appendix_pattern,
                )

                # --- Derive _t_ matching fields (A16 / T5) ---
                first_stop_id: str | None
                last_stop_id: str | None
                scheduled_start_time: datetime | None
                scheduled_end_time: datetime | None

                if is_complete_stop_sequence and stop_time_records:
                    first_stop_id = stop_time_records[0].stop_id
                    last_stop_id = stop_time_records[-1].stop_id
                else:
                    origin_ref = _strip_appendix(
                        _find_text(journey, ns, "OriginRef") or "", dpl_appendix_pattern
                    ).strip() or None
                    destination_ref = _strip_appendix(
                        _find_text(journey, ns, "DestinationRef") or "", dpl_appendix_pattern
                    ).strip() or None
                    first_stop_id = origin_ref
                    last_stop_id = destination_ref

                if stop_time_records:
                    first_call_raw = raw_calls[0]
                    last_call_raw = raw_calls[-1]
                    scheduled_start_time = _aimed_departure_or_arrival(first_call_raw, ns, self._processor_timezone)
                    scheduled_end_time = _aimed_arrival_or_departure(last_call_raw, ns, self._processor_timezone)
                else:
                    scheduled_start_time = None
                    scheduled_end_time = None

                journey_count += 1

                # --- Build trip record ---
                trip_record = TripRecord(
                    operation_day_date=operation_day,
                    trip_id=trip_id,
                    route_id=route_id,
                    operator_id=None,
                    operator_name=None,
                    concessionaire_id=operator_ref,
                    concessionaire_name=None,
                    schedule_relationship=trip_schedule_relationship,
                    realtime_pipeline_id=pipeline.id,
                    _t_scheduled_start_time=scheduled_start_time,
                    _t_scheduled_end_time=scheduled_end_time,
                    _t_scheduled_start_stop_id=first_stop_id,
                    _t_scheduled_end_stop_id=last_stop_id,
                    _t_is_complete_stop_sequence=is_complete_stop_sequence,
                )

                # --- Apply mapping ---
                mapped_trip, mapped_stop_times = await self._mapping_service.map_records_for_loading(
                    instance_id=instance.id,
                    pipeline_id=pipeline.id,
                    trip=trip_record,
                    stop_times=stop_time_records,
                )

                if trip_schedule_relationship != "CANCELED" and not mapped_stop_times:
                    continue

                # --- Quality monitoring ---
                self._monitor_quality_issues(
                    instance=instance,
                    pipeline=pipeline,
                    now_processor_tz=now_processor_tz,
                    trip_id=trip_id,
                    route_id=route_id,
                    operator_ref=operator_ref,
                    trip_record=trip_record,
                    stop_time_records=stop_time_records,
                    is_complete_stop_sequence=is_complete_stop_sequence,
                )

                # --- Load ---
                def loading_issue_handler(issue: RealtimeLoadingQualityIssue) -> None:
                    self.report_quality_issue(
                        instance=instance,
                        pipeline=pipeline,
                        timestamp=now_processor_tz,
                        entity_id=trip_id,
                        issue_type_id=issue.issue_type,
                        concessionaire_id=operator_ref,
                        assessment_value=issue.assessment_value,
                    )

                realtime_loading_result: RealtimeLoadingResult = (
                    await self._loading_service.load_realtime_trip_and_stop_times(
                        instance_id=instance.id,
                        trip=mapped_trip,
                        stop_times=mapped_stop_times,
                        issue_handler=loading_issue_handler,
                    )
                )

                if realtime_loading_result == RealtimeLoadingResult.SUCCESS_DIRECT:
                    loaded_direct_trip_count += 1
                elif realtime_loading_result == RealtimeLoadingResult.SUCCESS_MATCHED:
                    loaded_matched_trip_count += 1

            LOGGER.info(
                "siri_et_light_pipeline_loaded",
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                processor_timezone=self._processor_timezone_name,
                num_entities=journey_count,
                loaded_direct_trip_count=loaded_direct_trip_count,
                loaded_matched_trip_count=loaded_matched_trip_count,
            )

            self.report_request(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                num_entities=journey_count,
                loaded_direct_trip_count=loaded_direct_trip_count,
                loaded_matched_trip_count=loaded_matched_trip_count,
                status_code=200,
            )

        except HttpError as http_exc:
            LOGGER.exception(
                "siri_et_light_pipeline_http_error",
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
                status_code=http_exc.status_code,
            )

        except Exception as exc:
            LOGGER.exception(
                "siri_et_light_pipeline_error",
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
                status_code=0,
            )

        finally:
            await self.submit_quality_report(instance=instance)

    def _read_endpoint_payload(
        self,
        endpoint: str,
        authentication: object,
    ) -> bytes:
        """Fetch the raw XML payload from the configured endpoint."""
        headers = self._build_auth_headers(authentication)  # type: ignore[arg-type]

        cert_filename: str | None = authentication.cert if authentication is not None else None  # type: ignore[union-attr]
        key_filename: str | None = authentication.key if authentication is not None else None  # type: ignore[union-attr]

        with requests.get(
            endpoint,
            headers=headers,
            timeout=30,
            cert=(cert_filename, key_filename) if cert_filename and key_filename else None,
        ) as response:
            status_code = response.status_code
            if status_code != 200:
                raise HttpError(
                    status_code,
                    f"HTTP request to {endpoint} failed with status code {status_code}",
                )
            
            return response.content

    def _parse_xml(self, payload: bytes) -> ET.Element:
        """Parse raw bytes into an ElementTree root element."""
        
        try:
            return ET.fromstring(payload)
        except ET.ParseError as exc:
            raise SiriEtLightPipelineError("Failed to parse SIRI-ET XML payload.") from exc

    def _build_stop_time_records(
        self,
        operation_day: date,
        trip_id: str,
        raw_calls: list[ET.Element],
        trip_schedule_relationship: str,
        ns: dict[str, str],
        dpl_appendix_pattern: str | None,
    ) -> list[StopTimeRecord]:
        """Build stop time records from SIRI EstimatedCall / RecordedCall elements."""
        
        records: list[StopTimeRecord] = []

        for index, call in enumerate(raw_calls):

            # T8 / A17: skip added stops
            extra_call_text = _find_text(call, ns, "ExtraCall")
            if extra_call_text is not None and extra_call_text.lower() == "true":
                continue

            stop_point_ref = _strip_appendix(
                _find_text(call, ns, "StopPointRef") or "", dpl_appendix_pattern
            ).strip()
            if not stop_point_ref:
                continue

            # T7: stop-level schedule relationship
            call_cancellation_text = _find_text(call, ns, "Cancellation")
            if call_cancellation_text is not None and call_cancellation_text.lower() == "true":
                stop_schedule_relationship = "CANCELED"
            else:
                stop_schedule_relationship = (
                    "SCHEDULED"
                    if trip_schedule_relationship == "SCHEDULED"
                    else trip_schedule_relationship
                )

            # Nominal times: AimedDepartureTime / AimedArrivalTime (A12)
            aimed_arrival = _parse_iso_datetime(
                _find_text(call, ns, "AimedArrivalTime"), self._processor_timezone
            )
            
            aimed_departure = _parse_iso_datetime(
                _find_text(call, ns, "AimedDepartureTime"), self._processor_timezone
            )

            # For RecordedCall: ActualArrivalTime / ActualDepartureTime
            # For EstimatedCall: ExpectedArrivalTime / ExpectedDepartureTime (A13)
            actual_arrival = _parse_iso_datetime(
                _find_text(call, ns, "ActualArrivalTime"), self._processor_timezone
            ) or _parse_iso_datetime(
                _find_text(call, ns, "ExpectedArrivalTime"), self._processor_timezone
            )

            actual_departure = _parse_iso_datetime(
                _find_text(call, ns, "ActualDepartureTime"), self._processor_timezone
            ) or _parse_iso_datetime(
                _find_text(call, ns, "ExpectedDepartureTime"), self._processor_timezone
            )

            # Nominal fallbacks – act times fall back per spec mapping table:
            # act_arrival  → ExpectedArrivalTime → ExpectedDepartureTime → AimedArrivalTime → AimedDepartureTime
            # act_departure → ExpectedDepartureTime → ExpectedArrivalTime → AimedDepartureTime → AimedArrivalTime
            if actual_arrival is None:
                actual_arrival = actual_departure or aimed_arrival or aimed_departure

            if actual_departure is None:
                actual_departure = actual_arrival or aimed_departure or aimed_arrival

            # nom_arrival_time / nom_departure_time serve as placeholders;
            # LoadingService._apply_nominal_baseline overwrites them from the DB.
            nom_arrival = aimed_arrival or aimed_departure
            nom_departure = aimed_departure or aimed_arrival

            # Both nom times must be non-None; use the actual time as last resort placeholder.
            placeholder = actual_arrival or actual_departure
            if nom_arrival is None:
                nom_arrival = placeholder

            if nom_departure is None:
                nom_departure = placeholder

            if nom_arrival is None or nom_departure is None:
                # Cannot construct a valid stop time record without any time reference.
                continue

            # stop_sequence is 1-based implicit index (spec: not available in the data)
            stop_sequence = index + 1

            records.append(
                StopTimeRecord(
                    operation_day_date=operation_day,
                    trip_id=trip_id,
                    stop_id=stop_point_ref,
                    distance_from_start=0.0,
                    nom_arrival_time=nom_arrival,
                    nom_departure_time=nom_departure,
                    act_arrival_time=actual_arrival,
                    act_departure_time=actual_departure,
                    schedule_relationship=stop_schedule_relationship,
                    stop_sequence=stop_sequence,
                )
            )

        return records

    def _monitor_quality_issues(
        self,
        instance: InstanceConfig,
        pipeline: PipelineConfig,
        now_processor_tz: datetime,
        trip_id: str,
        route_id: str,
        operator_ref: str | None,
        trip_record: TripRecord,
        stop_time_records: list[StopTimeRecord],
        is_complete_stop_sequence: bool,
    ) -> None:
        """Report quality issues detected during processing of a single journey."""

        if not GlobalId.is_global_id(trip_id):
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.TripIdNonGlobal,
                concessionaire_id=operator_ref,
                assessment_value=trip_id,
            )

        if route_id == "UNKNOWN-ROUTE":
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.RouteIdIsNull,
                concessionaire_id=operator_ref,
            )
        elif not GlobalId.is_global_id(route_id):
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.RouteIdNonGlobal,
                concessionaire_id=operator_ref,
                assessment_value=route_id,
            )

        if trip_record.operation_day_date is None:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.OperationDayIsNull,
                concessionaire_id=operator_ref,
            )

        if not is_complete_stop_sequence:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.NotCompleteStopSequence,
                concessionaire_id=operator_ref,
            )

        if trip_record._t_scheduled_start_stop_id is None:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.StartStopIdNull,
                concessionaire_id=operator_ref,
            )

        if trip_record._t_scheduled_end_stop_id is None:
            self.report_quality_issue(
                instance=instance,
                pipeline=pipeline,
                timestamp=now_processor_tz,
                entity_id=trip_id,
                issue_type_id=QualityIssue.DestinationStopIdNull,
                concessionaire_id=operator_ref,
            )

        for stop_time_record in stop_time_records:
            if not GlobalId.is_global_id(stop_time_record.stop_id):
                self.report_quality_issue(
                    instance=instance,
                    pipeline=pipeline,
                    timestamp=now_processor_tz,
                    entity_id=trip_id,
                    issue_type_id=QualityIssue.StopIdNonGlobal,
                    concessionaire_id=operator_ref,
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
                    entity_id=trip_id,
                    issue_type_id=QualityIssue.EstimatedDepatureTimeBeforeArrivalTime,
                    concessionaire_id=operator_ref,
                    assessment_value=(
                        f"{stop_time_record.act_departure_time.isoformat()} < "
                        f"{stop_time_record.act_arrival_time.isoformat()}"
                    ),
                )

            if (
                stop_time_record.nom_arrival_time is not None
                and stop_time_record.nom_departure_time is not None
                and stop_time_record.nom_departure_time < stop_time_record.nom_arrival_time
            ):
                self.report_quality_issue(
                    instance=instance,
                    pipeline=pipeline,
                    timestamp=now_processor_tz,
                    entity_id=trip_id,
                    issue_type_id=QualityIssue.AimedDepartureTimeBeforeArrivalTime,
                    concessionaire_id=operator_ref,
                    assessment_value=(
                        f"{stop_time_record.nom_departure_time.isoformat()} < "
                        f"{stop_time_record.nom_arrival_time.isoformat()}"
                    ),
                )


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _detect_namespace(root: ET.Element) -> dict[str, str]:
    """Detect the SIRI namespace from the root element tag and return an ns-map."""
    
    tag = root.tag
    if tag.startswith("{"):
        ns_uri = tag[1 : tag.index("}")]
        return {"s": ns_uri}
    
    return {}


def _tag(ns: dict[str, str], local: str) -> str:
    """Build a fully qualified tag name using the detected namespace."""
    
    if "s" in ns:
        return f"{{{ns['s']}}}{local}"
    
    return local


def _find_text(element: ET.Element, ns: dict[str, str], local: str) -> str | None:
    """Return the stripped text content of a direct child element, or None."""
    
    child = element.find(_tag(ns, local))
    if child is None:
        return None
    
    text = child.text
    
    return text.strip() if text else None


def _find_element(element: ET.Element, ns: dict[str, str], *path: str) -> ET.Element | None:
    """Traverse a sequence of child tag names and return the last found element."""
    
    current: ET.Element = element
    for local in path:
        found = current.find(_tag(ns, local))
        if found is None:
            return None
        current = found

    return current


def _iter_estimated_vehicle_journeys(
    root: ET.Element,
    ns: dict[str, str],
) -> Iterator[ET.Element]:
    """Yield all EstimatedVehicleJourney elements from the feed."""
    
    for et_delivery in root.iter(_tag(ns, "EstimatedTimetableDelivery")):
        for frame in et_delivery.iter(_tag(ns, "EstimatedJourneyVersionFrame")):
            yield from frame.findall(_tag(ns, "EstimatedVehicleJourney"))


def _collect_calls(journey: ET.Element, ns: dict[str, str]) -> list[ET.Element]:
    """Collect all RecordedCall and EstimatedCall elements in stop order.

    SIRI structures them in two separate containers:
    - ``RecordedCalls`` → ``RecordedCall`` elements  (past stops, always first)
    - ``EstimatedCalls`` → ``EstimatedCall`` elements (future/current stops)

    Both containers are read and concatenated so that the resulting list
    preserves the correct chronological stop order.
    """

    calls: list[ET.Element] = []

    recorded_calls_elem = journey.find(_tag(ns, "RecordedCalls"))
    if recorded_calls_elem is not None:
        for child in recorded_calls_elem:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == "RecordedCall":
                calls.append(child)

    estimated_calls_elem = journey.find(_tag(ns, "EstimatedCalls"))
    if estimated_calls_elem is not None:
        for child in estimated_calls_elem:
            local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if local == "EstimatedCall":
                calls.append(child)

    return calls


# ---------------------------------------------------------------------------
# Time / date helpers
# ---------------------------------------------------------------------------

def _safe_zoneinfo(timezone_name: str) -> ZoneInfo:
    
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        LOGGER.warning("siri_et_light_invalid_timezone_fallback", timezone=timezone_name)
        return ZoneInfo("UTC")


def _parse_iso_datetime(raw_value: str | None, target_timezone: ZoneInfo) -> datetime | None:
    """Parse an ISO 8601 datetime string and convert it to target_timezone."""
    if not raw_value:
        return None
    try:
        dt = datetime.fromisoformat(raw_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=target_timezone)
        return dt.astimezone(target_timezone)
    except ValueError:
        return None


def _parse_time_of_day(raw_value: str) -> time:
    """Parse a HH:MM time string. Returns midnight on failure."""
    parts = raw_value.split(":")
    if len(parts) >= 2:
        try:
            return time(int(parts[0]), int(parts[1]))
        except ValueError:
            pass
    return _DEFAULT_OPERATION_DAY_END


def _derive_operation_day(
    journey: ET.Element,
    ns: dict[str, str],
    raw_calls: list[ET.Element],
    fallback_date: date,
    operation_day_end: time,
    processor_timezone: ZoneInfo,
) -> date:
    """Derive the operation day date for a vehicle journey (T2 / A2).

    Priority:
    1. FramedVehicleJourneyRef.DataFrameRef as YYYY-MM-DD
    2. First call's aimed departure time (or aimed arrival time as fallback),
       adjusted for overnight services via operation_day_end.
    3. fallback_date (today in processor timezone)
    """

    # 1. Try DataFrameRef
    framed_ref = journey.find(_tag(ns, "FramedVehicleJourneyRef"))
    if framed_ref is not None:
        data_frame_ref = _find_text(framed_ref, ns, "DataFrameRef")
        if data_frame_ref:
            parsed = _parse_date(data_frame_ref)
            if parsed is not None:
                return parsed

    # 2. Derive from first call's aimed time
    if raw_calls:
        first_call = raw_calls[0]
        aimed_text = _find_text(first_call, ns, "AimedDepartureTime") or _find_text(
            first_call, ns, "AimedArrivalTime"
        )

        aimed_dt = _parse_iso_datetime(aimed_text, processor_timezone)
        if aimed_dt is not None:

            call_time = aimed_dt.time().replace(tzinfo=None)

            # If the call time is before or equal to operation_day_end, the trip belongs
            # to the previous calendar day (overnight service rule).
            if call_time <= operation_day_end:
                return aimed_dt.date() - timedelta(days=1)
            
            return aimed_dt.date()

    return fallback_date


def _parse_date(raw_value: str) -> date | None:
    """Parse a YYYY-MM-DD date string, returning None on failure."""
    
    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        return None


def _aimed_departure_or_arrival(
    call: ET.Element,
    ns: dict[str, str],
    processor_timezone: ZoneInfo,
) -> datetime | None:
    """Return aimed departure time, falling back to aimed arrival time."""
    
    return _parse_iso_datetime(
        _find_text(call, ns, "AimedDepartureTime"), processor_timezone
    ) or _parse_iso_datetime(
        _find_text(call, ns, "AimedArrivalTime"), processor_timezone
    )


def _aimed_arrival_or_departure(
    call: ET.Element,
    ns: dict[str, str],
    processor_timezone: ZoneInfo,
) -> datetime | None:
    """Return aimed arrival time, falling back to aimed departure time."""
    
    return _parse_iso_datetime(
        _find_text(call, ns, "AimedArrivalTime"), processor_timezone
    ) or _parse_iso_datetime(
        _find_text(call, ns, "AimedDepartureTime"), processor_timezone
    )


def _strip_appendix(value: str, pattern: str | None) -> str:
    """Remove a data-platform appendix from an ID string if the pattern is set."""
    
    if not pattern or not value:
        return value
    
    idx = value.find(pattern)
    if idx >= 0:
        return value[:idx]
    
    return value


def _matches_filter(value: str, filter_rules: tuple[FilterEntryConfig, ...]) -> bool:
    """Apply include/exclude filter rules against a string value."""
    
    include_rules = [r for r in filter_rules if r.type == "include"]
    exclude_rules = [r for r in filter_rules if r.type == "exclude"]

    if include_rules and not any(fnmatch.fnmatchcase(value, r.match) for r in include_rules):
        return False

    if any(fnmatch.fnmatchcase(value, r.match) for r in exclude_rules):
        return False

    return True

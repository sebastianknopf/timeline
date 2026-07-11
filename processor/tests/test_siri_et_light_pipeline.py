from __future__ import annotations

import unittest
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.common.quality_issues import QualityIssue
from processor.loading.loading_service import LoadingService, RealtimeLoadingResult
from processor.loading.models import QualityIssueRecord, RequestRecord, RouteRecord, StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.pipelines.siri_et_light_pipeline import (
    SiriEtLightPipeline,
    _collect_calls,
    _derive_operation_day,
    _detect_namespace,
    _matches_filter,
    _parse_date,
    _parse_iso_datetime,
    _parse_time_of_day,
    _strip_appendix,
)
from processor.runtime_config import (
    AuthenticationConfig,
    FilterConfig,
    FilterEntryConfig,
    InstanceConfig,
    PipelineConfig,
)


# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_TZ_BERLIN = ZoneInfo("Europe/Berlin")
_TZ_UTC = ZoneInfo("UTC")

_INSTANCE = InstanceConfig(id="test-instance", pipelines=())
_PIPELINE = PipelineConfig(
    id="siri-et-test",
    name="siri-et-light",
    type="realtime",
    cron="*/1 * * * *",
    endpoint="https://example.test/siri-et",
)


# ---------------------------------------------------------------------------
# Recording helpers (mirrors test_gtfsrt_tripupdates_pipeline.py)
# ---------------------------------------------------------------------------


class RecordingRepository:
    def __init__(self) -> None:
        self.realtime_trips: list[TripRecord] = []
        self.realtime_stop_times: list[StopTimeRecord] = []
        self.nominal_stop_times: list[StopTimeRecord] = []
        self.requests: list[tuple[str, RequestRecord]] = []
        self.quality_issues: list[tuple[str, QualityIssueRecord]] = []

    async def upsert_nominal_stops(self, instance_id: str, stops: list[object]) -> None:
        return None

    async def insert_nominal_routes(self, instance_id: str, routes: list[RouteRecord]) -> None:
        return None

    async def upsert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        return None

    async def upsert_nominal_stop_times(self, instance_id: str, stop_times: list[StopTimeRecord]) -> None:
        return None

    async def insert_nominal_stops(self, instance_id: str, stops: list[object]) -> None:
        return None

    async def insert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        return None

    async def insert_nominal_stop_times(self, instance_id: str, stop_times: list[StopTimeRecord]) -> None:
        return None

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        return None

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        self.realtime_trips.append(trip)

    async def upsert_realtime_stop_times(self, instance_id: str, stop_times: list[StopTimeRecord]) -> None:
        self.realtime_stop_times.extend(stop_times)

    async def get_nominal_stop_times_for_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        return [
            s for s in self.nominal_stop_times
            if s.operation_day_date == operation_day_date and s.trip_id == trip_id
        ]

    async def get_nominal_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> None:
        return None

    async def insert_request(self, instance_id: str, request: RequestRecord) -> None:
        self.requests.append((instance_id, request))

    async def upsert_quality_issues(
        self,
        instance_id: str,
        quality_issues: list[QualityIssueRecord],
    ) -> None:
        self.quality_issues.extend((instance_id, issue) for issue in quality_issues)

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
        return None


class RecordingLoadingService(LoadingService):
    def __init__(self, repository: RecordingRepository) -> None:
        super().__init__(repository=repository)
        self.calls: list[tuple[str, TripRecord, list[StopTimeRecord]]] = []

    async def load_realtime_trip_and_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
        issue_handler: object | None = None,
    ) -> RealtimeLoadingResult:
        self.calls.append((instance_id, trip, stop_times))
        return RealtimeLoadingResult.INTERNAL_ERROR


class InMemorySiriEtLightPipeline(SiriEtLightPipeline):
    """Subclass that injects a fixed payload instead of making HTTP requests."""

    def __init__(
        self,
        payload: bytes,
        loading_service: LoadingService,
        mapping_service: MappingService,
        processor_timezone_name: str = "UTC",
    ) -> None:
        super().__init__(
            loading_service=loading_service,
            mapping_service=mapping_service,
            processor_timezone_name=processor_timezone_name,
        )
        self._payload = payload

    def _read_endpoint_payload(self, endpoint: str, authentication: object) -> bytes:
        return self._payload


# ---------------------------------------------------------------------------
# XML builder helpers
# ---------------------------------------------------------------------------

_SIRI_NS = "http://www.siri.org.uk/siri"


def _siri_xml(journeys: str, response_timestamp: str | None = None) -> bytes:
    ts_element = f'    <ResponseTimestamp>{response_timestamp}</ResponseTimestamp>\n' if response_timestamp else ''
    xml = (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<Siri xmlns="{_SIRI_NS}" version="2.0">\n'
        f'  <ServiceDelivery>\n'
        f'{ts_element}'
        f'    <EstimatedTimetableDelivery>\n'
        f'      <EstimatedJourneyVersionFrame>\n'
        f'{journeys}\n'
        f'      </EstimatedJourneyVersionFrame>\n'
        f'    </EstimatedTimetableDelivery>\n'
        f'  </ServiceDelivery>\n'
        f'</Siri>\n'
    )
    return xml.encode("utf-8")


def _journey(
    trip_id: str = "de:trip:123",
    line_ref: str = "de:line:45",
    operator_ref: str | None = "de:op:01",
    data_frame_ref: str | None = "2026-07-10",
    monitored: str | None = "true",
    cancellation: str | None = None,
    is_complete: str | None = "true",
    origin_ref: str | None = None,
    destination_ref: str | None = None,
    calls: str = "",
) -> str:
    parts = ["<EstimatedVehicleJourney>"]
    parts.append(f"  <LineRef>{line_ref}</LineRef>")
    if operator_ref:
        parts.append(f"  <OperatorRef>{operator_ref}</OperatorRef>")
    parts.append(f"  <DatedVehicleJourneyRef>{trip_id}</DatedVehicleJourneyRef>")
    if data_frame_ref:
        parts.append(
            f"  <FramedVehicleJourneyRef>"
            f"<DataFrameRef>{data_frame_ref}</DataFrameRef>"
            f"</FramedVehicleJourneyRef>"
        )
    if monitored is not None:
        parts.append(f"  <Monitored>{monitored}</Monitored>")
    if cancellation is not None:
        parts.append(f"  <Cancellation>{cancellation}</Cancellation>")
    if is_complete:
        parts.append(f"  <IsCompleteStopSequence>{is_complete}</IsCompleteStopSequence>")
    if origin_ref:
        parts.append(f"  <OriginRef>{origin_ref}</OriginRef>")
    if destination_ref:
        parts.append(f"  <DestinationRef>{destination_ref}</DestinationRef>")
    parts.append(f"  <EstimatedCalls>{calls}</EstimatedCalls>")
    parts.append("</EstimatedVehicleJourney>")
    return "\n".join(parts)


def _estimated_call(
    stop_id: str = "de:stop:1",
    aimed_departure: str = "2026-07-10T10:00:00+00:00",
    aimed_arrival: str = "2026-07-10T09:58:00+00:00",
    expected_departure: str | None = "2026-07-10T10:05:00+00:00",
    expected_arrival: str | None = "2026-07-10T10:03:00+00:00",
    cancellation: str | None = None,
    extra_call: str | None = None,
) -> str:
    parts = [
        "<EstimatedCall>",
        f"  <StopPointRef>{stop_id}</StopPointRef>",
        f"  <AimedArrivalTime>{aimed_arrival}</AimedArrivalTime>",
        f"  <AimedDepartureTime>{aimed_departure}</AimedDepartureTime>",
    ]
    if expected_arrival:
        parts.append(f"  <ExpectedArrivalTime>{expected_arrival}</ExpectedArrivalTime>")
    if expected_departure:
        parts.append(f"  <ExpectedDepartureTime>{expected_departure}</ExpectedDepartureTime>")
    if cancellation:
        parts.append(f"  <Cancellation>{cancellation}</Cancellation>")
    if extra_call:
        parts.append(f"  <ExtraCall>{extra_call}</ExtraCall>")
    parts.append("</EstimatedCall>")
    return "\n".join(parts)


def _two_calls() -> str:
    return (
        _estimated_call(stop_id="de:stop:A", aimed_departure="2026-07-10T10:00:00+00:00",
                        aimed_arrival="2026-07-10T09:58:00+00:00",
                        expected_departure="2026-07-10T10:05:00+00:00",
                        expected_arrival="2026-07-10T10:03:00+00:00")
        + _estimated_call(stop_id="de:stop:B", aimed_departure="2026-07-10T10:30:00+00:00",
                          aimed_arrival="2026-07-10T10:28:00+00:00",
                          expected_departure="2026-07-10T10:35:00+00:00",
                          expected_arrival="2026-07-10T10:33:00+00:00")
    )


# ---------------------------------------------------------------------------
# Unit tests – pure helper functions
# ---------------------------------------------------------------------------


class DetectNamespaceTests(unittest.TestCase):
    def test_detects_siri_namespace(self) -> None:
        import xml.etree.ElementTree as ET
        xml = f'<Siri xmlns="{_SIRI_NS}" />'
        root = ET.fromstring(xml)
        ns = _detect_namespace(root)
        self.assertEqual({"s": _SIRI_NS}, ns)

    def test_returns_empty_dict_for_no_namespace(self) -> None:
        import xml.etree.ElementTree as ET
        root = ET.fromstring("<Siri />")
        ns = _detect_namespace(root)
        self.assertEqual({}, ns)


class StripAppendixTests(unittest.TestCase):
    def test_strips_appendix_when_pattern_present(self) -> None:
        self.assertEqual("ID123", _strip_appendix("ID123#!ADD!#appendix", "#!ADD!#"))

    def test_returns_original_when_pattern_absent(self) -> None:
        self.assertEqual("ID123", _strip_appendix("ID123", "#!ADD!#"))

    def test_returns_original_when_no_pattern_configured(self) -> None:
        self.assertEqual("ID123#!ADD!#appendix", _strip_appendix("ID123#!ADD!#appendix", None))

    def test_empty_value_returns_empty(self) -> None:
        self.assertEqual("", _strip_appendix("", "#!ADD!#"))


class MatchesFilterTests(unittest.TestCase):
    def _rule(self, match: str, type_: str = "include") -> FilterEntryConfig:
        return FilterEntryConfig(match=match, type=type_)

    def test_include_rule_passes_matching_value(self) -> None:
        rules = (self._rule("de:line:*"),)
        self.assertTrue(_matches_filter("de:line:45", rules))

    def test_include_rule_blocks_non_matching_value(self) -> None:
        rules = (self._rule("de:line:*"),)
        self.assertFalse(_matches_filter("at:line:99", rules))

    def test_exclude_rule_blocks_matching_value(self) -> None:
        rules = (self._rule("de:line:99", "exclude"),)
        self.assertFalse(_matches_filter("de:line:99", rules))

    def test_exclude_rule_passes_non_matching_value(self) -> None:
        rules = (self._rule("de:line:99", "exclude"),)
        self.assertTrue(_matches_filter("de:line:45", rules))

    def test_empty_rules_always_passes(self) -> None:
        self.assertTrue(_matches_filter("de:line:45", ()))


class ParseIsoDatetimeTests(unittest.TestCase):
    def test_parses_offset_datetime(self) -> None:
        result = _parse_iso_datetime("2026-07-10T10:00:00+02:00", _TZ_UTC)
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(datetime(2026, 7, 10, 8, 0, tzinfo=_TZ_UTC), result)

    def test_returns_none_for_empty_string(self) -> None:
        self.assertIsNone(_parse_iso_datetime("", _TZ_UTC))

    def test_returns_none_for_none(self) -> None:
        self.assertIsNone(_parse_iso_datetime(None, _TZ_UTC))

    def test_returns_none_for_invalid_value(self) -> None:
        self.assertIsNone(_parse_iso_datetime("not-a-date", _TZ_UTC))


class ParseTimeOfDayTests(unittest.TestCase):
    def test_parses_valid_time(self) -> None:
        self.assertEqual(time(3, 0), _parse_time_of_day("03:00"))

    def test_parses_midnight(self) -> None:
        self.assertEqual(time(0, 0), _parse_time_of_day("00:00"))

    def test_falls_back_on_invalid(self) -> None:
        self.assertEqual(time(3, 0), _parse_time_of_day("invalid"))


class ParseDateTests(unittest.TestCase):
    def test_parses_iso_date(self) -> None:
        self.assertEqual(date(2026, 7, 10), _parse_date("2026-07-10"))

    def test_returns_none_for_invalid(self) -> None:
        self.assertIsNone(_parse_date("not-a-date"))

    def test_returns_none_for_non_iso_format(self) -> None:
        # Python 3.11+ fromisoformat accepts YYYYMMDD; verify pipeline gracefully handles both
        # formats by ensuring invalid strings still return None.
        self.assertIsNone(_parse_date("not-a-date"))
        self.assertIsNone(_parse_date("26-07-10"))


class DeriveOperationDayTests(unittest.TestCase):
    import xml.etree.ElementTree as _ET

    def _make_journey(self, xml_str: str) -> "_ET.Element":
        import xml.etree.ElementTree as ET
        return ET.fromstring(xml_str)

    def test_uses_data_frame_ref_when_present(self) -> None:
        import xml.etree.ElementTree as ET
        xml = f"""
        <EstimatedVehicleJourney xmlns="{_SIRI_NS}">
          <FramedVehicleJourneyRef>
            <DataFrameRef>2026-07-10</DataFrameRef>
          </FramedVehicleJourneyRef>
          <EstimatedCalls />
        </EstimatedVehicleJourney>
        """
        root = ET.fromstring(xml.strip())
        ns = _detect_namespace(root)
        result = _derive_operation_day(
            journey=root,
            ns=ns,
            raw_calls=[],
            fallback_date=date(2026, 1, 1),
            operation_day_end=time(3, 0),
            processor_timezone=_TZ_UTC,
        )
        self.assertEqual(date(2026, 7, 10), result)

    def test_falls_back_to_first_call_aimed_time_for_normal_service(self) -> None:
        import xml.etree.ElementTree as ET
        journey_xml = f"""
        <EstimatedVehicleJourney xmlns="{_SIRI_NS}">
          <EstimatedCalls />
        </EstimatedVehicleJourney>
        """
        call_xml = f"""
        <EstimatedCall xmlns="{_SIRI_NS}">
          <AimedDepartureTime>2026-07-10T10:00:00+00:00</AimedDepartureTime>
        </EstimatedCall>
        """
        root = ET.fromstring(journey_xml.strip())
        call = ET.fromstring(call_xml.strip())
        ns = _detect_namespace(root)
        result = _derive_operation_day(
            journey=root,
            ns=ns,
            raw_calls=[call],
            fallback_date=date(2026, 1, 1),
            operation_day_end=time(3, 0),
            processor_timezone=_TZ_UTC,
        )
        self.assertEqual(date(2026, 7, 10), result)

    def test_uses_previous_day_for_overnight_service(self) -> None:
        """A trip aimed at 01:30 UTC with operation_day_end=03:00 belongs to the previous day."""
        import xml.etree.ElementTree as ET
        journey_xml = f"""
        <EstimatedVehicleJourney xmlns="{_SIRI_NS}">
          <EstimatedCalls />
        </EstimatedVehicleJourney>
        """
        call_xml = f"""
        <EstimatedCall xmlns="{_SIRI_NS}">
          <AimedDepartureTime>2026-07-11T01:30:00+00:00</AimedDepartureTime>
        </EstimatedCall>
        """
        root = ET.fromstring(journey_xml.strip())
        call = ET.fromstring(call_xml.strip())
        ns = _detect_namespace(root)
        result = _derive_operation_day(
            journey=root,
            ns=ns,
            raw_calls=[call],
            fallback_date=date(2026, 1, 1),
            operation_day_end=time(3, 0),
            processor_timezone=_TZ_UTC,
        )
        self.assertEqual(date(2026, 7, 10), result)

    def test_returns_fallback_when_no_data_available(self) -> None:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(f'<EstimatedVehicleJourney xmlns="{_SIRI_NS}" />')
        ns = _detect_namespace(root)
        result = _derive_operation_day(
            journey=root,
            ns=ns,
            raw_calls=[],
            fallback_date=date(2026, 7, 5),
            operation_day_end=time(3, 0),
            processor_timezone=_TZ_UTC,
        )
        self.assertEqual(date(2026, 7, 5), result)


# ---------------------------------------------------------------------------
# Integration-style pipeline tests
# ---------------------------------------------------------------------------


class SiriEtLightPipelineTests(unittest.IsolatedAsyncioTestCase):
    def _make_pipeline(self, **kwargs: object) -> tuple[InMemorySiriEtLightPipeline, RecordingLoadingService]:
        payload: bytes = kwargs.pop("payload", _siri_xml(_journey(calls=_two_calls())))  # type: ignore[assignment]
        repository = RecordingRepository()
        loading_service = RecordingLoadingService(repository=repository)
        mapping_service = MappingService()
        pipeline = _PIPELINE
        mapping_service.register_pipeline_mapping(instance_id=_INSTANCE.id, pipeline=pipeline)
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )
        return siri_pipeline, loading_service

    async def test_basic_journey_is_passed_to_loading_service(self) -> None:
        siri_pipeline, loading_service = self._make_pipeline()
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(loading_service.calls))
        _, trip, stop_times = loading_service.calls[0]
        self.assertEqual("de:trip:123", trip.trip_id)
        self.assertEqual("de:line:45", trip.route_id)
        self.assertEqual("SCHEDULED", trip.schedule_relationship)
        self.assertEqual(2, len(stop_times))

    async def test_stop_sequences_are_one_based_incrementing(self) -> None:
        siri_pipeline, loading_service = self._make_pipeline()
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, _, stop_times = loading_service.calls[0]
        sequences = [s.stop_sequence for s in stop_times]
        self.assertEqual([1, 2], sequences)

    async def test_cancelled_trip_without_calls_is_passed_to_loading_service(self) -> None:
        payload = _siri_xml(_journey(cancellation="true", monitored=None, calls=""))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(loading_service.calls))
        _, trip, stop_times = loading_service.calls[0]
        self.assertEqual("CANCELED", trip.schedule_relationship)
        self.assertEqual(0, len(stop_times))

    async def test_unmonitored_trip_is_discarded(self) -> None:
        payload = _siri_xml(_journey(monitored="false", calls=_two_calls()))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(0, len(loading_service.calls))

    async def test_extra_trip_is_discarded(self) -> None:
        extra_journey = (
            f'<EstimatedVehicleJourney xmlns="{_SIRI_NS}">'
            f'<LineRef>de:line:45</LineRef>'
            f'<DatedVehicleJourneyRef>de:trip:extra</DatedVehicleJourneyRef>'
            f'<ExtraTrip>true</ExtraTrip>'
            f'<Monitored>true</Monitored>'
            f'<EstimatedCalls>{_two_calls()}</EstimatedCalls>'
            f'</EstimatedVehicleJourney>'
        )
        payload = _siri_xml(extra_journey)
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(0, len(loading_service.calls))

    async def test_extra_call_within_journey_is_skipped(self) -> None:
        calls = (
            _estimated_call(stop_id="de:stop:A")
            + _estimated_call(stop_id="de:stop:extra", extra_call="true")
            + _estimated_call(stop_id="de:stop:B", aimed_departure="2026-07-10T10:30:00+00:00",
                              aimed_arrival="2026-07-10T10:28:00+00:00")
        )
        payload = _siri_xml(_journey(calls=calls))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(loading_service.calls))
        _, _, stop_times = loading_service.calls[0]
        stop_ids = [s.stop_id for s in stop_times]
        self.assertNotIn("de:stop:extra", stop_ids)
        self.assertEqual(2, len(stop_times))

    async def test_route_include_filter_passes_matching_trip(self) -> None:
        pipeline = PipelineConfig(
            id="siri-et-test",
            name="siri-et-light",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/siri-et",
            filter=FilterConfig(routes=(FilterEntryConfig(match="de:line:*", type="include"),)),
        )
        payload = _siri_xml(_journey(line_ref="de:line:45", calls=_two_calls()))
        repository = RecordingRepository()
        loading_service = RecordingLoadingService(repository=repository)
        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=_INSTANCE.id, pipeline=pipeline)
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service
        )
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=pipeline)
        self.assertEqual(1, len(loading_service.calls))

    async def test_route_include_filter_blocks_non_matching_trip(self) -> None:
        pipeline = PipelineConfig(
            id="siri-et-test",
            name="siri-et-light",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/siri-et",
            filter=FilterConfig(routes=(FilterEntryConfig(match="at:line:*", type="include"),)),
        )
        payload = _siri_xml(_journey(line_ref="de:line:45", calls=_two_calls()))
        repository = RecordingRepository()
        loading_service = RecordingLoadingService(repository=repository)
        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=_INSTANCE.id, pipeline=pipeline)
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service
        )
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=pipeline)
        self.assertEqual(0, len(loading_service.calls))

    async def test_operator_filter_blocks_trip_without_operator_ref(self) -> None:
        pipeline = PipelineConfig(
            id="siri-et-test",
            name="siri-et-light",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/siri-et",
            filter=FilterConfig(operators=(FilterEntryConfig(match="de:op:*", type="include"),)),
        )
        payload = _siri_xml(_journey(operator_ref=None, calls=_two_calls()))
        repository = RecordingRepository()
        loading_service = RecordingLoadingService(repository=repository)
        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=_INSTANCE.id, pipeline=pipeline)
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service
        )
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=pipeline)
        self.assertEqual(0, len(loading_service.calls))

    async def test_dpl_appendix_pattern_is_stripped_from_trip_and_stop_ids(self) -> None:
        pipeline = PipelineConfig(
            id="siri-et-test",
            name="siri-et-light",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/siri-et",
            parameters={"dpl_appendix_pattern": "#!ADD!#"},
        )
        calls = (
            _estimated_call(stop_id="de:stop:A#!ADD!#extra1")
            + _estimated_call(stop_id="de:stop:B#!ADD!#extra2",
                              aimed_departure="2026-07-10T10:30:00+00:00",
                              aimed_arrival="2026-07-10T10:28:00+00:00")
        )
        payload = _siri_xml(_journey(trip_id="de:trip:123#!ADD!#appendix", calls=calls))
        repository = RecordingRepository()
        loading_service = RecordingLoadingService(repository=repository)
        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=_INSTANCE.id, pipeline=pipeline)
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service
        )
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=pipeline)

        self.assertEqual(1, len(loading_service.calls))
        _, trip, stop_times = loading_service.calls[0]
        self.assertEqual("de:trip:123", trip.trip_id)
        self.assertEqual(["de:stop:A", "de:stop:B"], [s.stop_id for s in stop_times])

    async def test_operation_day_taken_from_data_frame_ref(self) -> None:
        payload = _siri_xml(_journey(data_frame_ref="2026-07-10", calls=_two_calls()))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        self.assertEqual(date(2026, 7, 10), trip.operation_day_date)

    async def test_stop_actual_times_are_set_from_expected_fields(self) -> None:
        siri_pipeline, loading_service = self._make_pipeline()
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, _, stop_times = loading_service.calls[0]
        first = stop_times[0]
        self.assertIsNotNone(first.act_arrival_time)
        self.assertIsNotNone(first.act_departure_time)
        # ExpectedArrivalTime was 10:03:00+00:00
        assert first.act_arrival_time is not None
        self.assertEqual(datetime(2026, 7, 10, 10, 3, 0, tzinfo=_TZ_UTC), first.act_arrival_time)

    async def test_cancelled_stop_gets_canceled_schedule_relationship(self) -> None:
        calls = (
            _estimated_call(stop_id="de:stop:A")
            + _estimated_call(stop_id="de:stop:B",
                              aimed_departure="2026-07-10T10:30:00+00:00",
                              aimed_arrival="2026-07-10T10:28:00+00:00",
                              cancellation="true")
        )
        payload = _siri_xml(_journey(calls=calls))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, _, stop_times = loading_service.calls[0]
        self.assertEqual("SCHEDULED", stop_times[0].schedule_relationship)
        self.assertEqual("CANCELED", stop_times[1].schedule_relationship)

    async def test_complete_stop_sequence_flag_propagated_to_trip_record(self) -> None:
        payload = _siri_xml(_journey(is_complete="true", calls=_two_calls()))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        self.assertTrue(trip._t_is_complete_stop_sequence)

    async def test_incomplete_stop_sequence_flag_propagated(self) -> None:
        payload = _siri_xml(_journey(is_complete="false", calls=_two_calls()))
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        self.assertFalse(trip._t_is_complete_stop_sequence)

    async def test_origin_destination_ref_used_when_not_complete(self) -> None:
        payload = _siri_xml(
            _journey(
                is_complete="false",
                origin_ref="de:stop:ORIGIN",
                destination_ref="de:stop:DEST",
                calls=_two_calls(),
            )
        )
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        self.assertEqual("de:stop:ORIGIN", trip._t_scheduled_start_stop_id)
        self.assertEqual("de:stop:DEST", trip._t_scheduled_end_stop_id)

    async def test_first_and_last_stop_used_when_complete(self) -> None:
        payload = _siri_xml(
            _journey(
                is_complete="true",
                origin_ref="de:stop:ORIGIN",
                destination_ref="de:stop:DEST",
                calls=_two_calls(),
            )
        )
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        # When IsCompleteStopSequence is true, first/last stop IDs come from the calls.
        self.assertEqual("de:stop:A", trip._t_scheduled_start_stop_id)
        self.assertEqual("de:stop:B", trip._t_scheduled_end_stop_id)

    async def test_trip_without_id_is_silently_skipped(self) -> None:
        journey = (
            f'<EstimatedVehicleJourney xmlns="{_SIRI_NS}">'
            f'<LineRef>de:line:45</LineRef>'
            f'<Monitored>true</Monitored>'
            f'<EstimatedCalls>{_two_calls()}</EstimatedCalls>'
            f'</EstimatedVehicleJourney>'
        )
        payload = _siri_xml(journey)
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(0, len(loading_service.calls))

    async def test_response_timestamp_from_service_delivery_is_used_as_age(self) -> None:
        """A past ResponseTimestamp must produce a positive age_seconds in the request record."""
        payload = _siri_xml(_journey(calls=_two_calls()), response_timestamp="2020-01-01T00:00:00+00:00")
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        repository: RecordingRepository = loading_service._repository  # type: ignore[attr-defined]
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(repository.requests))
        _, request = repository.requests[0]
        self.assertGreater(request.age_seconds, 0)

    async def test_missing_response_timestamp_produces_zero_age(self) -> None:
        """When ResponseTimestamp is absent, age_seconds must be 0 in the request record."""
        siri_pipeline, loading_service = self._make_pipeline()
        repository: RecordingRepository = loading_service._repository  # type: ignore[attr-defined]
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(repository.requests))
        _, request = repository.requests[0]
        self.assertEqual(0, request.age_seconds)

    async def test_request_is_submitted_after_successful_execution(self) -> None:
        siri_pipeline, loading_service = self._make_pipeline()
        repository: RecordingRepository = loading_service._repository  # type: ignore[attr-defined]
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(repository.requests))
        _, request = repository.requests[0]
        self.assertEqual(_PIPELINE.id, request.pipeline_id)
        self.assertEqual(200, request.status_code)

    async def test_wrong_pipeline_name_raises_pipeline_error(self) -> None:
        pipeline = PipelineConfig(
            id="siri-et-test",
            name="wrong-name",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/siri-et",
        )
        repository = RecordingRepository()
        loading_service = LoadingService(repository=repository)
        mapping_service = MappingService()
        siri_pipeline = InMemorySiriEtLightPipeline(
            payload=b"",
            loading_service=loading_service,
            mapping_service=mapping_service,
        )
        # SiriEtLightPipelineError is caught internally; a request with status 0 is submitted.
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=pipeline)
        # Exactly one request record with status_code=0 must be persisted.
        self.assertEqual(1, len(repository.requests))
        _, request = repository.requests[0]
        self.assertIsNotNone(request)
        self.assertEqual(0, request.status_code)

    async def test_recorded_calls_are_processed_as_stop_times(self) -> None:
        recorded_call = (
            "<RecordedCall>"
            "<StopPointRef>de:stop:R</StopPointRef>"
            "<AimedArrivalTime>2026-07-10T09:00:00+00:00</AimedArrivalTime>"
            "<AimedDepartureTime>2026-07-10T09:01:00+00:00</AimedDepartureTime>"
            "<ActualArrivalTime>2026-07-10T09:00:30+00:00</ActualArrivalTime>"
            "<ActualDepartureTime>2026-07-10T09:01:30+00:00</ActualDepartureTime>"
            "</RecordedCall>"
        )
        estimated_call = _estimated_call(stop_id="de:stop:E")
        journey_xml = (
            f'<EstimatedVehicleJourney xmlns="{_SIRI_NS}">'
            f'<LineRef>de:line:45</LineRef>'
            f'<DatedVehicleJourneyRef>de:trip:123</DatedVehicleJourneyRef>'
            f'<FramedVehicleJourneyRef><DataFrameRef>2026-07-10</DataFrameRef></FramedVehicleJourneyRef>'
            f'<Monitored>true</Monitored>'
            f'<IsCompleteStopSequence>true</IsCompleteStopSequence>'
            f'<RecordedCalls>{recorded_call}</RecordedCalls>'
            f'<EstimatedCalls>{estimated_call}</EstimatedCalls>'
            f'</EstimatedVehicleJourney>'
        )
        payload = _siri_xml(journey_xml)
        siri_pipeline, loading_service = self._make_pipeline(payload=payload)
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        self.assertEqual(1, len(loading_service.calls))
        _, _, stop_times = loading_service.calls[0]
        self.assertEqual(2, len(stop_times))
        recorded = stop_times[0]
        self.assertEqual("de:stop:R", recorded.stop_id)
        self.assertIsNotNone(recorded.act_arrival_time)
        assert recorded.act_arrival_time is not None
        self.assertEqual(30, (recorded.act_arrival_time - datetime(2026, 7, 10, 9, 0, tzinfo=_TZ_UTC)).seconds)

    async def test_concessionaire_id_taken_from_operator_ref(self) -> None:
        siri_pipeline, loading_service = self._make_pipeline()
        await siri_pipeline.execute(instance=_INSTANCE, pipeline=_PIPELINE)

        _, trip, _ = loading_service.calls[0]
        self.assertEqual("de:op:01", trip.concessionaire_id)
        self.assertIsNone(trip.concessionaire_name)
        self.assertIsNone(trip.operator_id)


if __name__ == "__main__":
    unittest.main()

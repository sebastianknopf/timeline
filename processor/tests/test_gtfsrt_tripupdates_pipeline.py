from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.loading_service import LoadingService
from processor.loading.models import RouteRecord, StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.pipelines.gtfsrt_tripupdates_pipeline import GtfsRtTripUpdatesPipeline
from processor.runtime_config import FilterConfig, FilterEntryConfig, InstanceConfig, MappingConfig, PipelineConfig


class RecordingRepository:
    def __init__(self) -> None:
        self.realtime_trips: list[TripRecord] = []
        self.realtime_stop_times: list[StopTimeRecord] = []
        self.nominal_stop_times: list[StopTimeRecord] = []

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

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
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

    async def get_nominal_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> None:
        # Pipeline-level tests use stop-level distances directly; no nominal trip record needed.
        return None

    async def find_nominal_trip_id_by_properties(
        self,
        instance_id: str,
        operation_day_date: date,
        route_id: str,
        scheduled_start_time_str: str,
    ) -> str | None:
        return None


def make_pipeline(
    *,
    endpoint: str,
    mapping: MappingConfig | None = None,
    filter_config: FilterConfig | None = None,
) -> PipelineConfig:
    return PipelineConfig(
        id="realtime-main",
        name="gtfsrt-tripupdates",
        type="realtime",
        cron="*/1 * * * *",
        endpoint=endpoint,
        authentication=None,
        mapping=mapping,
        filter=filter_config,
    )


class InMemoryGtfsRtTripUpdatesPipeline(GtfsRtTripUpdatesPipeline):
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


class GtfsRtTripUpdatesPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_loads_mapped_realtime_trip_and_stop_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_mapping = tmp_dir / "routes.csv"
            routes_mapping.write_text("key,value\nR1,ROUTE-1\n", encoding="utf-8")
            stops_mapping = tmp_dir / "stops.csv"
            stops_mapping.write_text("key,value\nS1,STOP-A\nS2,STOP-B\n", encoding="utf-8")

            pipeline = make_pipeline(
                endpoint="https://example.test/realtime",
                mapping=MappingConfig(routes=routes_mapping, stops=stops_mapping),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            now = datetime.now(UTC)
            payload = _build_feed_payload(
                trip_id="T-1",
                route_id="R1",
                start_date="20260525",
                stop_updates=(
                    _StopUpdateInput(
                        stop_id="S1",
                        arrival_timestamp=now + timedelta(minutes=2),
                        departure_timestamp=now + timedelta(minutes=3),
                        stop_sequence=1,
                    ),
                    _StopUpdateInput(
                        stop_id="S2",
                        arrival_timestamp=now + timedelta(minutes=6),
                        departure_timestamp=now + timedelta(minutes=7),
                        stop_sequence=2,
                    ),
                ),
            )

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
            repository = RecordingRepository()
            repository.nominal_stop_times = [
                StopTimeRecord(
                    operation_day_date=datetime.strptime("20260525", "%Y%m%d").date(),
                    trip_id="T-1",
                    stop_id="STOP-A",
                    distance_from_start=0.0,
                    nom_arrival_time=now + timedelta(minutes=2),
                    nom_departure_time=now + timedelta(minutes=3),
                    act_arrival_time=None,
                    act_departure_time=None,
                    stop_sequence=1,
                ),
                StopTimeRecord(
                    operation_day_date=datetime.strptime("20260525", "%Y%m%d").date(),
                    trip_id="T-1",
                    stop_id="STOP-B",
                    distance_from_start=5.0,
                    nom_arrival_time=now + timedelta(minutes=6),
                    nom_departure_time=now + timedelta(minutes=7),
                    act_arrival_time=None,
                    act_departure_time=None,
                    stop_sequence=2,
                ),
            ]
            loading_service = LoadingService(repository=repository)

            gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
                payload=payload,
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.realtime_trips))
            self.assertEqual(2, len(repository.realtime_stop_times))

            loaded_trip = repository.realtime_trips[0]
            self.assertEqual("ROUTE-1", loaded_trip.route_id)
            self.assertEqual("STOP-A", loaded_trip.nom_start_stop_id)
            self.assertEqual("STOP-B", loaded_trip.nom_end_stop_id)
            self.assertEqual("T-1", loaded_trip.trip_id)
            self.assertEqual(5.0, loaded_trip.act_total_distance)

            loaded_stop_ids = {item.stop_id for item in repository.realtime_stop_times}
            self.assertEqual({"STOP-A", "STOP-B"}, loaded_stop_ids)

    async def test_pipeline_applies_route_filter_wildcard_include(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_mapping = tmp_dir / "routes.csv"
            routes_mapping.write_text("key,value\nR1A,ROUTE-1\nR2B,ROUTE-2\n", encoding="utf-8")
            stops_mapping = tmp_dir / "stops.csv"
            stops_mapping.write_text("key,value\nS1,STOP-A\nS2,STOP-B\n", encoding="utf-8")

            pipeline = make_pipeline(
                endpoint="https://example.test/realtime",
                mapping=MappingConfig(routes=routes_mapping, stops=stops_mapping),
                filter_config=FilterConfig(
                    routes=(
                        FilterEntryConfig(match="R1*", type="include"),
                    ),
                ),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            now = datetime.now(UTC)
            payload = _build_feed_payload(
                trip_id="T-1",
                route_id="R1A",
                start_date="20260525",
                stop_updates=(
                    _StopUpdateInput(
                        stop_id="S1",
                        arrival_timestamp=now + timedelta(minutes=2),
                        departure_timestamp=now + timedelta(minutes=3),
                        stop_sequence=1,
                    ),
                ),
            )

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
            repository = RecordingRepository()
            repository.nominal_stop_times = [
                StopTimeRecord(
                    operation_day_date=datetime.strptime("20260525", "%Y%m%d").date(),
                    trip_id="T-1",
                    stop_id="STOP-A",
                    distance_from_start=0.0,
                    nom_arrival_time=now + timedelta(minutes=2),
                    nom_departure_time=now + timedelta(minutes=3),
                    act_arrival_time=None,
                    act_departure_time=None,
                    stop_sequence=1,
                ),
            ]
            loading_service = LoadingService(repository=repository)

            gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
                payload=payload,
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.realtime_trips))
            self.assertEqual("ROUTE-1", repository.realtime_trips[0].route_id)

    async def test_pipeline_applies_route_filter_wildcard_exclude(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_mapping = tmp_dir / "routes.csv"
            routes_mapping.write_text("key,value\nR1A,ROUTE-1\nR2B,ROUTE-2\n", encoding="utf-8")
            stops_mapping = tmp_dir / "stops.csv"
            stops_mapping.write_text("key,value\nS1,STOP-A\nS2,STOP-B\n", encoding="utf-8")

            pipeline = make_pipeline(
                endpoint="https://example.test/realtime",
                mapping=MappingConfig(routes=routes_mapping, stops=stops_mapping),
                filter_config=FilterConfig(
                    routes=(
                        FilterEntryConfig(match="R2*", type="exclude"),
                    ),
                ),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            now = datetime.now(UTC)
            payload = _build_feed_payload(
                trip_id="T-2",
                route_id="R1A",
                start_date="20260525",
                stop_updates=(
                    _StopUpdateInput(
                        stop_id="S1",
                        arrival_timestamp=now + timedelta(minutes=2),
                        departure_timestamp=now + timedelta(minutes=3),
                        stop_sequence=1,
                    ),
                ),
            )

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
            repository = RecordingRepository()
            repository.nominal_stop_times = [
                StopTimeRecord(
                    operation_day_date=datetime.strptime("20260525", "%Y%m%d").date(),
                    trip_id="T-2",
                    stop_id="STOP-A",
                    distance_from_start=0.0,
                    nom_arrival_time=now + timedelta(minutes=2),
                    nom_departure_time=now + timedelta(minutes=3),
                    act_arrival_time=None,
                    act_departure_time=None,
                    stop_sequence=1,
                ),
            ]
            loading_service = LoadingService(repository=repository)

            gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
                payload=payload,
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.realtime_trips))
            self.assertEqual("ROUTE-1", repository.realtime_trips[0].route_id)

    async def test_pipeline_discards_route_filtered_trip_without_route_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_mapping = tmp_dir / "routes.csv"
            routes_mapping.write_text("key,value\nR1A,ROUTE-1\n", encoding="utf-8")
            stops_mapping = tmp_dir / "stops.csv"
            stops_mapping.write_text("key,value\nS1,STOP-A\n", encoding="utf-8")

            pipeline = make_pipeline(
                endpoint="https://example.test/realtime",
                mapping=MappingConfig(routes=routes_mapping, stops=stops_mapping),
                filter_config=FilterConfig(
                    routes=(
                        FilterEntryConfig(match="R1*", type="include"),
                    ),
                ),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            now = datetime.now(UTC)
            payload = _build_feed_payload(
                trip_id="T-NO-ROUTE",
                route_id=None,
                start_date="20260525",
                stop_updates=(
                    _StopUpdateInput(
                        stop_id="S1",
                        arrival_timestamp=now + timedelta(minutes=2),
                        departure_timestamp=now + timedelta(minutes=3),
                        stop_sequence=1,
                    ),
                ),
            )

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
            repository = RecordingRepository()
            repository.nominal_stop_times = [
                StopTimeRecord(
                    operation_day_date=datetime.strptime("20260525", "%Y%m%d").date(),
                    trip_id="T-NO-ROUTE",
                    stop_id="STOP-A",
                    distance_from_start=0.0,
                    nom_arrival_time=now + timedelta(minutes=2),
                    nom_departure_time=now + timedelta(minutes=3),
                    act_arrival_time=None,
                    act_departure_time=None,
                    stop_sequence=1,
                ),
            ]
            loading_service = LoadingService(repository=repository)

            gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
                payload=payload,
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(0, len(repository.realtime_trips))
            self.assertEqual(0, len(repository.realtime_stop_times))

    async def test_pipeline_processes_past_only_stop_updates(self) -> None:
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        payload = _build_feed_payload(
            trip_id="T-PAST",
            route_id="R2",
            start_date="20260525",
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    arrival_timestamp=now - timedelta(minutes=5),
                    departure_timestamp=now - timedelta(minutes=4),
                    stop_sequence=1,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=date(2026, 5, 25),
                trip_id="T-PAST",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=now - timedelta(minutes=5),
                nom_departure_time=now - timedelta(minutes=4),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_trips))
        self.assertEqual(1, len(repository.realtime_stop_times))
        self.assertEqual("T-PAST", repository.realtime_trips[0].trip_id)

    async def test_pipeline_converts_event_times_to_processor_timezone(self) -> None:
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        base_utc = datetime.now(UTC) + timedelta(hours=2)
        arrival_utc = base_utc
        departure_utc = base_utc + timedelta(minutes=5)

        payload = _build_feed_payload(
            trip_id="T-TZ",
            route_id="R-TZ",
            start_date=arrival_utc.strftime("%Y%m%d"),
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    arrival_timestamp=arrival_utc,
                    departure_timestamp=departure_utc,
                    stop_sequence=1,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=arrival_utc.date(),
                trip_id="T-TZ",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=arrival_utc.replace(microsecond=0),
                nom_departure_time=departure_utc.replace(microsecond=0),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
            processor_timezone_name="Europe/Berlin",
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_stop_times))
        loaded_stop_time = repository.realtime_stop_times[0]
        self.assertEqual(
            arrival_utc.replace(microsecond=0).astimezone(ZoneInfo("Europe/Berlin")),
            loaded_stop_time.act_arrival_time,
        )
        self.assertEqual(
            departure_utc.replace(microsecond=0).astimezone(ZoneInfo("Europe/Berlin")),
            loaded_stop_time.act_departure_time,
        )

    async def test_pipeline_prefers_event_timestamp_over_delay_and_uses_delay_fallback(self) -> None:
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        trip_timestamp_utc = datetime.now(UTC) + timedelta(hours=2)
        arrival_timestamp_utc = trip_timestamp_utc + timedelta(minutes=2)

        payload = _build_feed_payload(
            trip_id="T-PREF",
            route_id="R-PREF",
            start_date=trip_timestamp_utc.strftime("%Y%m%d"),
            trip_timestamp=trip_timestamp_utc,
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    arrival_timestamp=arrival_timestamp_utc,
                    arrival_delay_seconds=900,
                    departure_delay_seconds=180,
                    stop_sequence=1,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=trip_timestamp_utc.date(),
                trip_id="T-PREF",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=arrival_timestamp_utc.replace(microsecond=0),
                nom_departure_time=arrival_timestamp_utc.replace(microsecond=0),
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
            processor_timezone_name="Europe/Berlin",
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_stop_times))
        loaded_stop_time = repository.realtime_stop_times[0]

        expected_arrival = arrival_timestamp_utc.replace(microsecond=0).astimezone(ZoneInfo("Europe/Berlin"))
        expected_departure = (
            arrival_timestamp_utc.replace(microsecond=0) + timedelta(seconds=180)
        ).astimezone(ZoneInfo("Europe/Berlin"))

        self.assertEqual(expected_arrival, loaded_stop_time.act_arrival_time)
        self.assertEqual(expected_departure, loaded_stop_time.act_departure_time)

    async def test_trip_act_boundaries_follow_first_and_last_stop_order(self) -> None:
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        first_departure = now + timedelta(minutes=20)
        last_arrival = now + timedelta(minutes=10)

        payload = _build_feed_payload(
            trip_id="T-ORDER",
            route_id="R-ORDER",
            start_date=now.strftime("%Y%m%d"),
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    stop_sequence=1,
                    departure_timestamp=first_departure,
                ),
                _StopUpdateInput(
                    stop_id="S2",
                    stop_sequence=2,
                    arrival_timestamp=last_arrival,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-ORDER",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=first_departure,
                nom_departure_time=first_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-ORDER",
                stop_id="S2",
                distance_from_start=3.0,
                nom_arrival_time=last_arrival,
                nom_departure_time=last_arrival,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_trips))
        trip = repository.realtime_trips[0]

        self.assertEqual(first_departure.replace(microsecond=0), trip.act_start_time)
        self.assertEqual(last_arrival.replace(microsecond=0), trip.act_end_time)


    async def test_delay_propagates_forward_between_explicit_updates(self) -> None:
        """Delay from S1 (on-time) propagates to S2; delay from S3 (+300s) propagates to S4."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        op_day = date(2026, 5, 25)
        nom_base = datetime(2026, 5, 25, 8, 0, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=10)
        nom_s1_dep = nom_base + timedelta(minutes=11)
        nom_s2_arr = nom_base + timedelta(minutes=20)
        nom_s2_dep = nom_base + timedelta(minutes=21)
        nom_s3_arr = nom_base + timedelta(minutes=30)
        nom_s3_dep = nom_base + timedelta(minutes=31)
        nom_s4_arr = nom_base + timedelta(minutes=40)
        nom_s4_dep = nom_base + timedelta(minutes=41)

        # GTFS-RT only delivers S1 (on time, delay=0) and S3 (+300 s late).
        payload = _build_feed_payload(
            trip_id="T-PROP",
            route_id="R-PROP",
            start_date="20260525",
            stop_updates=(
                _StopUpdateInput(stop_id="S1", stop_sequence=1, arrival_delay_seconds=0, departure_delay_seconds=0),
                _StopUpdateInput(stop_id="S3", stop_sequence=3, arrival_delay_seconds=300, departure_delay_seconds=300),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-PROP", stop_id="S1", distance_from_start=0.0,
                           nom_arrival_time=nom_s1_arr, nom_departure_time=nom_s1_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-PROP", stop_id="S2", distance_from_start=2.0,
                           nom_arrival_time=nom_s2_arr, nom_departure_time=nom_s2_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=2),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-PROP", stop_id="S3", distance_from_start=4.0,
                           nom_arrival_time=nom_s3_arr, nom_departure_time=nom_s3_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=3),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-PROP", stop_id="S4", distance_from_start=6.0,
                           nom_arrival_time=nom_s4_arr, nom_departure_time=nom_s4_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=4),
        ]
        loading_service = LoadingService(repository=repository)
        pipeline_instance = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service,
            processor_timezone_name="UTC",
        )

        await pipeline_instance.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(4, len(repository.realtime_stop_times))

        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}

        # S1 explicit on-time: act = nom (delay = 0)
        self.assertEqual(nom_s1_arr, by_seq[1].act_arrival_time)
        self.assertEqual(nom_s1_dep, by_seq[1].act_departure_time)

        # S2 propagated from S1 (delay = 0): act = nom, schedule_relationship = SCHEDULED
        self.assertEqual(nom_s2_arr, by_seq[2].act_arrival_time)
        self.assertEqual(nom_s2_dep, by_seq[2].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[2].schedule_relationship)

        # S3 explicit +300 s late
        self.assertEqual(nom_s3_arr + timedelta(seconds=300), by_seq[3].act_arrival_time)
        self.assertEqual(nom_s3_dep + timedelta(seconds=300), by_seq[3].act_departure_time)

        # S4 propagated from S3 (delay = +300 s): schedule_relationship = SCHEDULED
        self.assertEqual(nom_s4_arr + timedelta(seconds=300), by_seq[4].act_arrival_time)
        self.assertEqual(nom_s4_dep + timedelta(seconds=300), by_seq[4].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[4].schedule_relationship)

    async def test_on_time_first_stop_propagates_to_all_subsequent_stops(self) -> None:
        """If only the first stop is delivered on-time (delay=0), all other stops are expanded on-time with SCHEDULED."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        op_day = date(2026, 5, 25)
        nom_base = datetime(2026, 5, 25, 9, 0, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=5)
        nom_s1_dep = nom_base + timedelta(minutes=6)
        nom_s2_arr = nom_base + timedelta(minutes=15)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_arr = nom_base + timedelta(minutes=25)
        nom_s3_dep = nom_base + timedelta(minutes=26)

        # Only S1 delivered on time (delay = 0 on both sides).
        payload = _build_feed_payload(
            trip_id="T-ONTIME",
            route_id="R-ONTIME",
            start_date="20260525",
            stop_updates=(
                _StopUpdateInput(stop_id="S1", stop_sequence=1, arrival_delay_seconds=0, departure_delay_seconds=0),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-ONTIME", stop_id="S1", distance_from_start=0.0,
                           nom_arrival_time=nom_s1_arr, nom_departure_time=nom_s1_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-ONTIME", stop_id="S2", distance_from_start=3.0,
                           nom_arrival_time=nom_s2_arr, nom_departure_time=nom_s2_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=2),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-ONTIME", stop_id="S3", distance_from_start=6.0,
                           nom_arrival_time=nom_s3_arr, nom_departure_time=nom_s3_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=3),
        ]
        loading_service = LoadingService(repository=repository)
        pipeline_instance = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service,
            processor_timezone_name="UTC",
        )

        await pipeline_instance.execute(instance=instance, pipeline=pipeline)

        # All three stops must be written because S2 and S3 are propagated from S1.
        self.assertEqual(3, len(repository.realtime_stop_times))

        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}

        # S1 explicit on-time.
        self.assertEqual(nom_s1_arr, by_seq[1].act_arrival_time)
        self.assertEqual(nom_s1_dep, by_seq[1].act_departure_time)

        # S2 propagated: act = nom (delay = 0), SCHEDULED.
        self.assertEqual(nom_s2_arr, by_seq[2].act_arrival_time)
        self.assertEqual(nom_s2_dep, by_seq[2].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[2].schedule_relationship)

        # S3 propagated: act = nom (delay = 0), SCHEDULED.
        self.assertEqual(nom_s3_arr, by_seq[3].act_arrival_time)
        self.assertEqual(nom_s3_dep, by_seq[3].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[3].schedule_relationship)

    async def test_negative_delay_propagates_forward(self) -> None:
        """Early arrival (negative delay) is propagated to all subsequent stops without explicit updates."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        op_day = date(2026, 5, 25)
        nom_base = datetime(2026, 5, 25, 10, 0, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=10)
        nom_s1_dep = nom_base + timedelta(minutes=11)
        nom_s2_arr = nom_base + timedelta(minutes=20)
        nom_s2_dep = nom_base + timedelta(minutes=21)

        # S1 is 2 minutes early (delay = -120 s).
        payload = _build_feed_payload(
            trip_id="T-EARLY",
            route_id="R-EARLY",
            start_date="20260525",
            stop_updates=(
                _StopUpdateInput(stop_id="S1", stop_sequence=1, arrival_delay_seconds=-120, departure_delay_seconds=-120),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-EARLY", stop_id="S1", distance_from_start=0.0,
                           nom_arrival_time=nom_s1_arr, nom_departure_time=nom_s1_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-EARLY", stop_id="S2", distance_from_start=5.0,
                           nom_arrival_time=nom_s2_arr, nom_departure_time=nom_s2_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=2),
        ]
        loading_service = LoadingService(repository=repository)
        pipeline_instance = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service,
            processor_timezone_name="UTC",
        )

        await pipeline_instance.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(2, len(repository.realtime_stop_times))
        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}

        # S1: 2 min early.
        self.assertEqual(nom_s1_arr - timedelta(seconds=120), by_seq[1].act_arrival_time)

        # S2 propagated: also 2 min early, SCHEDULED.
        self.assertEqual(nom_s2_arr - timedelta(seconds=120), by_seq[2].act_arrival_time)
        self.assertEqual(nom_s2_dep - timedelta(seconds=120), by_seq[2].act_departure_time)
        self.assertEqual("SCHEDULED", by_seq[2].schedule_relationship)

    async def test_stops_before_first_explicit_update_are_not_propagated(self) -> None:
        """Stops that come before the first explicit update receive no propagated delay."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        op_day = date(2026, 5, 25)
        nom_base = datetime(2026, 5, 25, 11, 0, 0, tzinfo=UTC)

        nom_s1_arr = nom_base + timedelta(minutes=5)
        nom_s1_dep = nom_base + timedelta(minutes=6)
        nom_s2_arr = nom_base + timedelta(minutes=15)
        nom_s2_dep = nom_base + timedelta(minutes=16)
        nom_s3_arr = nom_base + timedelta(minutes=25)
        nom_s3_dep = nom_base + timedelta(minutes=26)

        # Only S2 is delivered (delay = +180 s); S1 has no update, S3 has no update.
        payload = _build_feed_payload(
            trip_id="T-MID",
            route_id="R-MID",
            start_date="20260525",
            stop_updates=(
                _StopUpdateInput(stop_id="S2", stop_sequence=2, arrival_delay_seconds=180, departure_delay_seconds=180),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(operation_day_date=op_day, trip_id="T-MID", stop_id="S1", distance_from_start=0.0,
                           nom_arrival_time=nom_s1_arr, nom_departure_time=nom_s1_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=1),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-MID", stop_id="S2", distance_from_start=3.0,
                           nom_arrival_time=nom_s2_arr, nom_departure_time=nom_s2_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=2),
            StopTimeRecord(operation_day_date=op_day, trip_id="T-MID", stop_id="S3", distance_from_start=6.0,
                           nom_arrival_time=nom_s3_arr, nom_departure_time=nom_s3_dep,
                           act_arrival_time=None, act_departure_time=None, stop_sequence=3),
        ]
        loading_service = LoadingService(repository=repository)
        pipeline_instance = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload, loading_service=loading_service, mapping_service=mapping_service,
            processor_timezone_name="UTC",
        )

        await pipeline_instance.execute(instance=instance, pipeline=pipeline)

        # Only S2 (explicit) and S3 (propagated from S2) should be written.
        # S1 has no preceding explicit update and therefore receives no propagated data.
        self.assertEqual(2, len(repository.realtime_stop_times))
        stop_seqs = {s.stop_sequence for s in repository.realtime_stop_times}
        self.assertIn(2, stop_seqs)
        self.assertIn(3, stop_seqs)
        self.assertNotIn(1, stop_seqs)

        by_seq = {s.stop_sequence: s for s in repository.realtime_stop_times}
        self.assertEqual(nom_s2_arr + timedelta(seconds=180), by_seq[2].act_arrival_time)
        self.assertEqual(nom_s3_arr + timedelta(seconds=180), by_seq[3].act_arrival_time)
        self.assertEqual("SCHEDULED", by_seq[3].schedule_relationship)

    async def test_act_end_time_written_for_future_last_stop(self) -> None:
        """act_end_time is written immediately when realtime data for the last stop arrives, even when that stop is in the future."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        first_departure = now - timedelta(minutes=5)
        last_arrival = now + timedelta(minutes=30)

        payload = _build_feed_payload(
            trip_id="T-FUTURE-END",
            route_id="R-FUTURE",
            start_date=now.strftime("%Y%m%d"),
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    stop_sequence=1,
                    departure_timestamp=first_departure,
                ),
                _StopUpdateInput(
                    stop_id="S2",
                    stop_sequence=2,
                    arrival_timestamp=last_arrival,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-FUTURE-END",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=first_departure,
                nom_departure_time=first_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-FUTURE-END",
                stop_id="S2",
                distance_from_start=3.0,
                nom_arrival_time=last_arrival,
                nom_departure_time=last_arrival,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_trips))
        trip = repository.realtime_trips[0]

        self.assertEqual(first_departure.replace(microsecond=0), trip.act_start_time)
        self.assertIsNotNone(trip.act_end_time)
        self.assertEqual(last_arrival.replace(microsecond=0), trip.act_end_time)

    async def test_act_end_time_prefers_arrival_over_departure_at_last_stop(self) -> None:
        """act_end_time uses arrival of last stop when both arrival and departure are present."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        last_arrival = now - timedelta(minutes=5)
        last_departure = now - timedelta(minutes=2)

        payload = _build_feed_payload(
            trip_id="T-ARR-PREF",
            route_id="R-ARR",
            start_date=now.strftime("%Y%m%d"),
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    stop_sequence=1,
                    departure_timestamp=now - timedelta(minutes=20),
                ),
                _StopUpdateInput(
                    stop_id="S2",
                    stop_sequence=2,
                    arrival_timestamp=last_arrival,
                    departure_timestamp=last_departure,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        first_dep = now - timedelta(minutes=20)
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-ARR-PREF",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=first_dep,
                nom_departure_time=first_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=now.date(),
                trip_id="T-ARR-PREF",
                stop_id="S2",
                distance_from_start=4.0,
                nom_arrival_time=last_arrival,
                nom_departure_time=last_departure,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_trips))
        trip = repository.realtime_trips[0]

        # act_end_time must use arrival, not departure, for the last stop.
        self.assertEqual(last_arrival.replace(microsecond=0), trip.act_end_time)

    async def test_act_start_uses_nom_departure_of_first_nominal_stop_when_no_realtime_coverage(self) -> None:
        """act_start_time falls back to nom_departure_time of the first nominal stop when
        that stop has no realtime coverage and no preceding delay to propagate.

        The nominal first stop (S1) has no realtime data — it is absent from
        normalized_stop_times.  act_start_time must be anchored to S1's nom_departure_time
        rather than using S2's actual departure (which would produce a mid-trip start time).
        act_end_time must use the last covered stop — S3 propagated from S2.
        """
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="*/1 * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        nom_base = now + timedelta(hours=2)
        delay_s = 180

        payload = _build_feed_payload(
            trip_id="T-MID-ONLY",
            route_id="R-MID",
            start_date=nom_base.strftime("%Y%m%d"),
            stop_updates=(
                # Only S2 is in the feed; S1 has no preceding update, S3 gets propagated.
                _StopUpdateInput(
                    stop_id="S2",
                    stop_sequence=2,
                    arrival_delay_seconds=delay_s,
                    departure_delay_seconds=delay_s,
                ),
            ),
        )

        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)
        repository = RecordingRepository()
        nom_s1_dep = nom_base + timedelta(minutes=5)
        nom_s2_arr = nom_base + timedelta(minutes=20)
        nom_s2_dep = nom_base + timedelta(minutes=21)
        nom_s3_arr = nom_base + timedelta(minutes=35)
        nom_s3_dep = nom_base + timedelta(minutes=36)
        repository.nominal_stop_times = [
            StopTimeRecord(
                operation_day_date=nom_base.date(),
                trip_id="T-MID-ONLY",
                stop_id="S1",
                distance_from_start=0.0,
                nom_arrival_time=nom_base + timedelta(minutes=4),
                nom_departure_time=nom_s1_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=1,
            ),
            StopTimeRecord(
                operation_day_date=nom_base.date(),
                trip_id="T-MID-ONLY",
                stop_id="S2",
                distance_from_start=5.0,
                nom_arrival_time=nom_s2_arr,
                nom_departure_time=nom_s2_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=2,
            ),
            StopTimeRecord(
                operation_day_date=nom_base.date(),
                trip_id="T-MID-ONLY",
                stop_id="S3",
                distance_from_start=10.0,
                nom_arrival_time=nom_s3_arr,
                nom_departure_time=nom_s3_dep,
                act_arrival_time=None,
                act_departure_time=None,
                stop_sequence=3,
            ),
        ]
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(1, len(repository.realtime_trips))
        trip = repository.realtime_trips[0]

        # S1 has no realtime data and no preceding propagation → absent from normalized.
        # act_start_time must fall back to S1's nom_departure_time, not S2's actual departure.
        expected_start = nom_s1_dep
        self.assertEqual(expected_start, trip.act_start_time)

        # S3 is propagated (SCHEDULED) from S2 → act_end_time uses S3's arrival.
        expected_end = (nom_s3_arr + timedelta(seconds=delay_s))
        self.assertEqual(expected_end, trip.act_end_time)

    async def test_added_trip_is_discarded_and_not_loaded(self) -> None:
        """ADDED trips must be completely discarded at the pipeline level."""
        pipeline = PipelineConfig(
            id="realtime-main",
            name="gtfsrt-tripupdates",
            type="realtime",
            cron="* * * * *",
            endpoint="https://example.test/realtime",
        )
        instance = InstanceConfig(id="demo", pipelines=(pipeline,))

        now = datetime.now(UTC)
        payload = _build_feed_payload(
            trip_id="T-ADDED",
            route_id="R-ADDED",
            start_date=now.strftime("%Y%m%d"),
            schedule_relationship=1,
            stop_updates=(
                _StopUpdateInput(
                    stop_id="S1",
                    arrival_timestamp=now + timedelta(minutes=1),
                    departure_timestamp=now + timedelta(minutes=2),
                    stop_sequence=1,
                ),
            ),
        )

        repository = RecordingRepository()
        loading_service = LoadingService(repository=repository)
        mapping_service = MappingService()
        mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual(0, len(repository.realtime_trips))
        self.assertEqual(0, len(repository.realtime_stop_times))


class _StopUpdateInput:
    def __init__(
        self,
        stop_id: str,
        stop_sequence: int,
        arrival_timestamp: datetime | None = None,
        departure_timestamp: datetime | None = None,
        arrival_delay_seconds: int | None = None,
        departure_delay_seconds: int | None = None,
    ) -> None:
        self.stop_id = stop_id
        self.arrival_timestamp = arrival_timestamp
        self.departure_timestamp = departure_timestamp
        self.arrival_delay_seconds = arrival_delay_seconds
        self.departure_delay_seconds = departure_delay_seconds
        self.stop_sequence = stop_sequence


def _build_feed_payload(
    trip_id: str,
    route_id: str | None,
    start_date: str,
    stop_updates: tuple[_StopUpdateInput, ...],
    trip_timestamp: datetime | None = None,
    schedule_relationship: int = 0,
) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now(UTC).timestamp())

    entity = feed.entity.add()
    entity.id = f"trip-{trip_id}"

    trip_update = entity.trip_update
    trip_update.trip.trip_id = trip_id
    if route_id is not None:
        trip_update.trip.route_id = route_id
    trip_update.trip.start_date = start_date
    trip_update.trip.schedule_relationship = schedule_relationship
    if trip_timestamp is not None:
        trip_update.timestamp = int(trip_timestamp.timestamp())

    for item in stop_updates:
        stop_update = trip_update.stop_time_update.add()
        stop_update.stop_id = item.stop_id
        stop_update.stop_sequence = item.stop_sequence

        if item.arrival_timestamp is not None:
            stop_update.arrival.time = int(item.arrival_timestamp.timestamp())
        if item.arrival_delay_seconds is not None:
            stop_update.arrival.delay = item.arrival_delay_seconds

        if item.departure_timestamp is not None:
            stop_update.departure.time = int(item.departure_timestamp.timestamp())
        if item.departure_delay_seconds is not None:
            stop_update.departure.delay = item.departure_delay_seconds

    return feed.SerializeToString()


if __name__ == "__main__":
    unittest.main()

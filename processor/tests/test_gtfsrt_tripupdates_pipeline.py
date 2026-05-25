from __future__ import annotations

from datetime import UTC, datetime, timedelta
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
from processor.loading.models import StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.pipelines.gtfsrt_tripupdates_pipeline import GtfsRtTripUpdatesPipeline
from processor.runtime_config import InstanceConfig, MappingConfig, PipelineConfig


class RecordingRepository:
    def __init__(self) -> None:
        self.realtime_trips: list[TripRecord] = []
        self.realtime_stop_times: list[StopTimeRecord] = []

    async def upsert_nominal_stops(self, instance_id: str, stops: list[object]) -> None:
        return None

    async def upsert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        return None

    async def upsert_nominal_stop_times(self, instance_id: str, stop_times: list[StopTimeRecord]) -> None:
        return None

    async def insert_nominal_stops(self, instance_id: str, stops: list[object]) -> None:
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

            pipeline = PipelineConfig(
                id="realtime-main",
                name="gtfsrt-tripupdates",
                type="realtime",
                cron="*/1 * * * *",
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
            self.assertEqual(2.0, loaded_trip.act_total_distance)

            loaded_stop_ids = {item.stop_id for item in repository.realtime_stop_times}
            self.assertEqual({"STOP-A", "STOP-B"}, loaded_stop_ids)

    async def test_pipeline_skips_past_only_stop_updates(self) -> None:
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
        loading_service = LoadingService(repository=repository)

        gtfsrt_pipeline = InMemoryGtfsRtTripUpdatesPipeline(
            payload=payload,
            loading_service=loading_service,
            mapping_service=mapping_service,
        )

        await gtfsrt_pipeline.execute(instance=instance, pipeline=pipeline)

        self.assertEqual([], repository.realtime_trips)
        self.assertEqual([], repository.realtime_stop_times)

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
        self.assertIsNone(trip.act_end_time)


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
    route_id: str,
    start_date: str,
    stop_updates: tuple[_StopUpdateInput, ...],
    trip_timestamp: datetime | None = None,
) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now(UTC).timestamp())

    entity = feed.entity.add()
    entity.id = f"trip-{trip_id}"

    trip_update = entity.trip_update
    trip_update.trip.trip_id = trip_id
    trip_update.trip.route_id = route_id
    trip_update.trip.start_date = start_date
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

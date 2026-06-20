from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta, tzinfo
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
from zoneinfo import ZoneInfo

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

import processor.pipelines.gtfs_pipeline as _gtfs_pipeline_module
from processor.loading.loading_service import LoadingService
from processor.loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.pipelines.gtfs_pipeline import GtfsNominalPipeline
from processor.runtime_config import FilterConfig, FilterEntryConfig, InstanceConfig, MappingConfig, PipelineConfig


class RecordingRepository:
    def __init__(self) -> None:
        self.stops: list[StopRecord] = []
        self.routes: list[RouteRecord] = []
        self.trips: list[TripRecord] = []
        self.stop_times: list[StopTimeRecord] = []

    async def upsert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        self.stops.extend(stops)

    async def insert_nominal_routes(self, instance_id: str, routes: list[RouteRecord]) -> None:
        self.routes.extend(routes)

    async def upsert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        self.trips.extend(trips)

    async def upsert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.stop_times.extend(stop_times)

    async def insert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        await self.upsert_nominal_stops(instance_id=instance_id, stops=stops)

    async def insert_nominal_trips(self, instance_id: str, trips: list[TripRecord]) -> None:
        self.trips.extend(trips)

    async def insert_nominal_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        self.stop_times.extend(stop_times)

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.insert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.insert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

    async def upsert_realtime_trip(self, instance_id: str, trip: TripRecord) -> None:
        return None

    async def upsert_realtime_stop_times(
        self,
        instance_id: str,
        stop_times: list[StopTimeRecord],
    ) -> None:
        return None

    async def get_nominal_stop_times_for_trip(
        self,
        instance_id: str,
        operation_day_date: date,
        trip_id: str,
    ) -> list[StopTimeRecord]:
        return []


class IdentityMappingService:
    def register_pipeline_mapping(self, instance_id: str, pipeline: object) -> None:
        return None

    async def map_stop_records(
        self,
        instance_id: str,
        pipeline_id: str,
        stops: list[StopRecord],
    ) -> list[StopRecord]:
        return stops

    async def map_records_for_loading(
        self,
        instance_id: str,
        pipeline_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> tuple[TripRecord, list[StopTimeRecord]]:
        return trip, stop_times


def make_pipeline(
    *,
    pipeline_id: str,
    endpoint: str,
    filter_config: FilterConfig | None = None,
    authentication: object | None = None,
    parameters: dict[str, object] | None = None,
) -> PipelineConfig:
    return PipelineConfig(
        id=pipeline_id,
        name="gtfs",
        type="nominal",
        cron="0 2 * * *",
        endpoint=endpoint,
        authentication=authentication,
        parameters=parameters or {},
        filter=filter_config,
    )


class GtfsNominalPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_pipeline_extracts_maps_and_loads_nominal_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name,route_long_name\nR1,A1,R1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop One,48.1,8.1,0\n"
                    "S2,Stop Two,48.2,8.2,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S2,2,10\n",
                )

            stops_mapping = tmp_dir / "stops.csv"
            stops_mapping.write_text("key,value\nS1,STOP-A\nS2,STOP-B\n", encoding="utf-8")
            routes_mapping = tmp_dir / "routes.csv"
            routes_mapping.write_text("key,value\nR1,ROUTE-1\n", encoding="utf-8")

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
                mapping=MappingConfig(stops=stops_mapping, routes=routes_mapping),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(2, len(repository.stops))
            self.assertEqual(1, len(repository.routes))
            self.assertEqual(1, len(repository.trips))
            self.assertEqual(2, len(repository.stop_times))
            self.assertEqual({"STOP-A", "STOP-B"}, {item.stop_id for item in repository.stops})
            self.assertEqual("ROUTE-1", repository.trips[0].route_id)
            self.assertEqual("ROUTE-1", repository.routes[0].route_id)
            self.assertEqual("R1", repository.routes[0].route_name)  # route_short_name takes priority (A8)
            self.assertEqual("STOP-A", repository.trips[0].nom_start_stop_id)
            self.assertEqual("STOP-B", repository.trips[0].nom_end_stop_id)

    async def test_pipeline_applies_route_filter_wildcard_include(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-route-filter.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1A,A1,Route 1\nR2B,A1,Route 2\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1A,SVC,T1\nR2B,SVC,T2\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T1,08:00:00,08:01:00,S1,1\n"
                    "T1,08:30:00,08:31:00,S2,2\n"
                    "T2,09:00:00,09:01:00,S1,1\n"
                    "T2,09:30:00,09:31:00,S2,2\n",
                )

            pipeline = make_pipeline(
                pipeline_id="nominal-main",
                endpoint=gtfs_zip.as_uri(),
                filter_config=FilterConfig(
                    routes=(
                        FilterEntryConfig(match="R1*", type="include"),
                    ),
                ),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=IdentityMappingService(),
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.routes))
            self.assertEqual(1, len(repository.trips))
            self.assertEqual("R1A", repository.routes[0].route_id)
            self.assertEqual("T1", repository.trips[0].trip_id)

    async def test_pipeline_applies_operator_filter_wildcard_exclude(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-operator-filter.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\nB1,Agency Two,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,Route 1\nR2,B1,Route 2\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\nR2,SVC,T2\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T1,08:00:00,08:01:00,S1,1\n"
                    "T1,08:30:00,08:31:00,S2,2\n"
                    "T2,09:00:00,09:01:00,S1,1\n"
                    "T2,09:30:00,09:31:00,S2,2\n",
                )

            pipeline = make_pipeline(
                pipeline_id="nominal-main",
                endpoint=gtfs_zip.as_uri(),
                filter_config=FilterConfig(
                    operators=(
                        FilterEntryConfig(match="B*", type="exclude"),
                    ),
                ),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=IdentityMappingService(),
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.routes))
            self.assertEqual(1, len(repository.trips))
            self.assertEqual("R1", repository.routes[0].route_id)
            self.assertEqual("T1", repository.trips[0].trip_id)

    async def test_pipeline_uses_processor_timezone_for_operation_day_date(self) -> None:
        # Regression guard for A4: operation_day_date must reflect the current date in
        # PROCESSOR_TIMEZONE, never UTC.
        #
        # Scenario: processor runs at 2026-05-28 00:30 Europe/Berlin (CEST, UTC+2),
        # which is 2026-05-27 22:30 UTC.
        #
        # 2026-05-28 is a Thursday; the GTFS calendar activates only on Thursdays.
        # If UTC were used, the resolved date would be 2026-05-27 (Wednesday) and no
        # trips would pass the service-calendar filter — the pipeline would load nothing.
        # Only correct use of PROCESSOR_TIMEZONE (Berlin) yields date(2026, 5, 28) and
        # therefore produces the expected loaded records.
        berlin_tz = ZoneInfo("Europe/Berlin")
        # 2026-05-28 00:30 Berlin = 2026-05-27 22:30 UTC
        fixed_now_berlin = datetime(2026, 5, 28, 0, 30, 0, tzinfo=berlin_tz)

        class _FixedNowDatetime(datetime):
            @classmethod
            def now(cls, tz: tzinfo | None = None) -> datetime:  # type: ignore[override]
                if tz is not None:
                    return fixed_now_berlin.astimezone(tz)
                return fixed_now_berlin

        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)

            gtfs_zip = tmp_dir / "gtfs-opday-tz.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,Europe/Berlin\n",
                )
                archive.writestr(
                    "calendar.txt",
                    # Service SVC-TH only runs on Thursdays (weekday index 3).
                    # date(2026, 5, 28).weekday() == 3  -> Thursday  -> trips loaded.
                    # date(2026, 5, 27).weekday() == 2  -> Wednesday -> no trips (UTC-wrong path).
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    "SVC-TH,0,0,0,1,0,0,0,20260101,20261231\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,R1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC-TH,T-OPDAY\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop 1,48.1,8.1,0\nS2,Stop 2,48.2,8.2,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T-OPDAY,08:00:00,08:01:00,S1,1\n"
                    "T-OPDAY,09:00:00,09:01:00,S2,2\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
                processor_timezone_name="Europe/Berlin",
            )

            with patch.object(_gtfs_pipeline_module, "datetime", _FixedNowDatetime):
                await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            # Trips must have been loaded (calendar activates on Thursday = Berlin date).
            self.assertEqual(1, len(repository.trips), "No trips loaded — operation_day_date timezone is wrong")
            self.assertEqual(2, len(repository.stop_times))

            # operation_day_date must be the Berlin date (2026-05-28), not the UTC date (2026-05-27).
            self.assertEqual(date(2026, 5, 28), repository.trips[0].operation_day_date)
            for stop_time in repository.stop_times:
                self.assertEqual(date(2026, 5, 28), stop_time.operation_day_date)

    async def test_pipeline_converts_gtfs_times_to_processor_timezone(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)

            gtfs_zip = tmp_dir / "gtfs-timezone.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    "SVC,1,1,1,1,1,1,1,20000101,20991231\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name,route_long_name\nR1,A1,R1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop One,48.1,8.1,0\n"
                    "S2,Stop Two,48.2,8.2,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S2,2,10\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
                processor_timezone_name="Europe/Berlin",
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            self.assertEqual(2, len(repository.stop_times))

            operation_day = repository.trips[0].operation_day_date
            expected_first_arrival = (
                datetime.combine(operation_day, time(0, 0), tzinfo=ZoneInfo("UTC"))
                + timedelta(hours=8)
            ).astimezone(ZoneInfo("Europe/Berlin"))

            self.assertEqual(expected_first_arrival, repository.stop_times[0].nom_arrival_time)
            self.assertEqual("Europe/Berlin", str(repository.stop_times[0].nom_arrival_time.tzinfo))

    async def test_pipeline_uses_processor_timezone_when_agency_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)

            gtfs_zip = tmp_dir / "gtfs-no-agency.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    "SVC,1,1,1,1,1,1,1,20000101,20991231\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,route_short_name,route_long_name\nR1,R1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop One,48.1,8.1,0\n"
                    "S2,Stop Two,48.2,8.2,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S2,2,10\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
                processor_timezone_name="Europe/Berlin",
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            self.assertEqual(2, len(repository.stop_times))
            self.assertEqual("Europe/Berlin", str(repository.stop_times[0].nom_arrival_time.tzinfo))

    async def test_pipeline_normalizes_shape_distance_values_to_kilometers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-distance.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name,route_long_name\nR1,A1,R1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop One,48.1,8.1,0\n"
                    "S2,Stop Two,48.2,8.2,0\n"
                    "S3,Stop Three,48.3,8.3,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S2,2,1500\n"
                    "T1,08:40:00,08:41:00,S3,3,\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(3, len(repository.stop_times))
            by_sequence = {item.stop_sequence: item for item in repository.stop_times}
            self.assertEqual(0.0, by_sequence[1].distance_from_start)
            self.assertEqual(1.5, by_sequence[2].distance_from_start)
            self.assertEqual(0.0, by_sequence[3].distance_from_start)
            self.assertEqual(1.5, repository.trips[0].nom_total_distance)

    async def test_pipeline_creates_placeholder_stop_for_referenced_missing_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-missing-stop.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name,route_long_name\nR1,A1,R1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
                    "S1,Stop One,48.1,8.1,0\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S-MISSING,2,10\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            stops_by_id = {item.stop_id: item for item in repository.stops}
            self.assertIn("S-MISSING", stops_by_id)
            self.assertEqual("S-MISSING", stops_by_id["S-MISSING"].stop_name)
            self.assertEqual(0.0, stops_by_id["S-MISSING"].stop_lat)
            self.assertEqual(0.0, stops_by_id["S-MISSING"].stop_lon)
            self.assertEqual("S-MISSING", repository.trips[0].nom_end_stop_id)

    async def test_pipeline_uses_shape_dist_traveled_from_shapes_txt_as_fallback(self) -> None:
        """A17 / T8: when stop_times has no shape_dist_traveled, nom_total_distance
        and distance_from_start must fall back to the shape index built from shapes.txt.
        The shape index is expected to use the max shape_dist_traveled value from
        shapes.txt, applying the meter-to-km heuristic.
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-shape-index.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,Route 1\n",
                )
                # trips.txt references shape SH1.
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id,shape_id\nR1,SVC,T1,SH1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                # stop_times intentionally has NO shape_dist_traveled column.
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T1,08:00:00,08:01:00,S1,1\n"
                    "T1,08:30:00,08:31:00,S2,2\n",
                )
                # shapes.txt: shape_dist_traveled in meters (max 12500 → 12.5 km after heuristic).
                archive.writestr(
                    "shapes.txt",
                    "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n"
                    "SH1,48.1,8.1,1,0\n"
                    "SH1,48.15,8.15,2,6250\n"
                    "SH1,48.2,8.2,3,12500\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            # 12500 m → 12.5 km via meter-detection heuristic (> 200 → /1000).
            self.assertAlmostEqual(12.5, repository.trips[0].nom_total_distance, places=5)

    async def test_pipeline_uses_coordinate_based_shape_index_when_shape_dist_traveled_absent(self) -> None:
        """A17: when shapes.txt has no shape_dist_traveled column at all, the shape
        index must fall back to Haversine-accumulated coordinate distance.  The
        resulting nom_total_distance should be a reasonable positive value.
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-shape-coords.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id,shape_id\nR1,SVC,T1,SH2\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T1,08:00:00,08:01:00,S1,1\n"
                    "T1,08:30:00,08:31:00,S2,2\n",
                )
                # shapes.txt: no shape_dist_traveled; only coordinates.
                # (48.1, 8.1) → (48.2, 8.2) is roughly ~13.5 km.
                archive.writestr(
                    "shapes.txt",
                    "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
                    "SH2,48.1,8.1,1\n"
                    "SH2,48.2,8.2,2\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            # Haversine distance between (48.1, 8.1) and (48.2, 8.2) is ~ 13.5 km.
            # Assert a positive value within a plausible range.
            nom_dist = repository.trips[0].nom_total_distance
            self.assertGreater(nom_dist, 5.0)
            self.assertLess(nom_dist, 30.0)

    async def test_pipeline_shape_dist_traveled_takes_priority_over_shape_index(self) -> None:
        """When stop_times.txt carries valid shape_dist_traveled values, those must
        remain the primary source for nom_total_distance even when shapes.txt is
        also present with a different distance.
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-shape-priority.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,Route 1\n",
                )
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id,shape_id\nR1,SVC,T1,SH3\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                # stop_times carries shape_dist_traveled (km scale: max 8.0 km).
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
                    "T1,08:00:00,08:01:00,S1,1,0\n"
                    "T1,08:30:00,08:31:00,S2,2,8.0\n",
                )
                # shapes.txt has a very different value (999 km) — must NOT be used.
                archive.writestr(
                    "shapes.txt",
                    "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled\n"
                    "SH3,48.1,8.1,1,0\n"
                    "SH3,48.2,8.2,2,999\n",
                )

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            # stop_times shape_dist_traveled (8.0 km) must take priority over shape index (999 km).
            self.assertAlmostEqual(8.0, repository.trips[0].nom_total_distance, places=5)

    async def test_pipeline_works_without_shapes_file(self) -> None:
        """When shapes.txt is absent the shape index is empty and the pipeline
        must still complete successfully, falling back to 0.0 for trips without
        shape_dist_traveled in stop_times.
        """
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            operation_day = datetime.now(UTC).date().strftime("%Y%m%d")

            gtfs_zip = tmp_dir / "gtfs-no-shapes.zip"
            with zipfile.ZipFile(gtfs_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr(
                    "agency.txt",
                    "agency_id,agency_name,agency_timezone\nA1,Agency One,UTC\n",
                )
                archive.writestr(
                    "calendar.txt",
                    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                    f"SVC,1,1,1,1,1,1,1,{operation_day},{operation_day}\n",
                )
                archive.writestr(
                    "routes.txt",
                    "route_id,agency_id,route_short_name\nR1,A1,Route 1\n",
                )
                # trips.txt has no shape_id.
                archive.writestr(
                    "trips.txt",
                    "route_id,service_id,trip_id\nR1,SVC,T1\n",
                )
                archive.writestr(
                    "stops.txt",
                    "stop_id,stop_name,stop_lat,stop_lon\n"
                    "S1,Stop One,48.1,8.1\nS2,Stop Two,48.2,8.2\n",
                )
                archive.writestr(
                    "stop_times.txt",
                    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
                    "T1,08:00:00,08:01:00,S1,1\n"
                    "T1,08:30:00,08:31:00,S2,2\n",
                )
                # No shapes.txt in archive.

            pipeline = PipelineConfig(
                id="nominal-main",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint=gtfs_zip.as_uri(),
            )
            instance = InstanceConfig(id="demo", pipelines=(pipeline,))

            mapping_service = MappingService()
            mapping_service.register_pipeline_mapping(instance_id=instance.id, pipeline=pipeline)

            repository = RecordingRepository()
            loading_service = LoadingService(repository=repository)
            gtfs_pipeline = GtfsNominalPipeline(
                loading_service=loading_service,
                mapping_service=mapping_service,
            )

            await gtfs_pipeline.execute(instance=instance, pipeline=pipeline)

            self.assertEqual(1, len(repository.trips))
            self.assertEqual(0.0, repository.trips[0].nom_total_distance)


if __name__ == "__main__":
    unittest.main()

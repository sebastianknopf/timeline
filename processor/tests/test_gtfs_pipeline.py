from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
import tempfile
import unittest
import zipfile
from zoneinfo import ZoneInfo

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.loading_service import LoadingService
from processor.loading.models import StopRecord, StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.pipelines.gtfs_pipeline import GtfsNominalPipeline
from processor.runtime_config import InstanceConfig, MappingConfig, PipelineConfig


class RecordingRepository:
    def __init__(self) -> None:
        self.stops: list[StopRecord] = []
        self.trips: list[TripRecord] = []
        self.stop_times: list[StopTimeRecord] = []

    async def upsert_nominal_stops(self, instance_id: str, stops: list[StopRecord]) -> None:
        self.stops.extend(stops)

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

    async def insert_nominal_trip_with_stop_times(
        self,
        instance_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> None:
        await self.upsert_nominal_trips(instance_id=instance_id, trips=[trip])
        await self.upsert_nominal_stop_times(instance_id=instance_id, stop_times=stop_times)

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
            self.assertEqual(1, len(repository.trips))
            self.assertEqual(2, len(repository.stop_times))
            self.assertEqual({"STOP-A", "STOP-B"}, {item.stop_id for item in repository.stops})
            self.assertEqual("ROUTE-1", repository.trips[0].route_id)
            self.assertEqual("R1", repository.trips[0].route_name)
            self.assertEqual("STOP-A", repository.trips[0].nom_start_stop_id)
            self.assertEqual("STOP-B", repository.trips[0].nom_end_stop_id)

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


if __name__ == "__main__":
    unittest.main()

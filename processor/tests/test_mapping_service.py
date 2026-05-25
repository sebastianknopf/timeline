from __future__ import annotations

from datetime import UTC, date, datetime
import tempfile
from pathlib import Path
import unittest

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.loading.models import StopRecord, StopTimeRecord, TripRecord
from processor.mapping.mapping_service import MappingService
from processor.runtime_config import MappingConfig, PipelineConfig


class MappingServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_map_route_id_uses_exact_and_wildcard_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_csv = tmp_dir / "routes.csv"
            routes_csv.write_text(
                "key,value\nR1,ROUTE-1\nR*,ROUTE-WILDCARD\n",
                encoding="utf-8",
            )

            pipeline = PipelineConfig(
                id="realtime",
                name="gtfsrt-tripupdates",
                type="realtime",
                cron="* * * * *",
                endpoint="https://example.test/realtime",
                mapping=MappingConfig(routes=routes_csv),
            )

            service = MappingService()
            service.register_pipeline_mapping(instance_id="demo", pipeline=pipeline)

            self.assertEqual("ROUTE-1", await service.map_route_id("demo", "realtime", "R1"))
            self.assertEqual(
                "ROUTE-WILDCARD",
                await service.map_route_id("demo", "realtime", "R-UNKNOWN"),
            )

    async def test_map_stop_records_returns_mapped_stop_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            stops_csv = tmp_dir / "stops.csv"
            stops_csv.write_text("key,value\nS1,STOP-A\n", encoding="utf-8")

            pipeline = PipelineConfig(
                id="nominal",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint="https://example.test/nominal",
                mapping=MappingConfig(stops=stops_csv),
            )

            service = MappingService()
            service.register_pipeline_mapping(instance_id="demo", pipeline=pipeline)

            mapped_stops = await service.map_stop_records(
                instance_id="demo",
                pipeline_id="nominal",
                stops=[
                    StopRecord(stop_id="S1", stop_name="A", stop_lat=1.0, stop_lon=2.0),
                    StopRecord(stop_id="S9", stop_name="B", stop_lat=3.0, stop_lon=4.0),
                ],
            )

            self.assertEqual("STOP-A", mapped_stops[0].stop_id)
            self.assertEqual("S9", mapped_stops[1].stop_id)

    async def test_map_records_for_loading_returns_mapped_loading_models(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            routes_csv = tmp_dir / "routes.csv"
            routes_csv.write_text("key,value\nR1,ROUTE-1\n", encoding="utf-8")

            stops_csv = tmp_dir / "stops.csv"
            stops_csv.write_text("key,value\nS1,STOP-A\nS2,STOP-B\n", encoding="utf-8")

            pipeline = PipelineConfig(
                id="nominal",
                name="gtfs",
                type="nominal",
                cron="0 2 * * *",
                endpoint="https://example.test/nominal",
                mapping=MappingConfig(stops=stops_csv, routes=routes_csv),
            )

            trip = TripRecord(
                operation_day_date=date(2026, 5, 24),
                trip_id="trip-1",
                route_id="R1",
                route_name="Route 1",
                concessionaire_id="conc-1",
                concessionaire_name="Concessionaire 1",
                operator_id="op-1",
                operator_name="Operator 1",
                nom_start_time=datetime(2026, 5, 24, 8, 0, tzinfo=UTC),
                nom_end_time=datetime(2026, 5, 24, 9, 0, tzinfo=UTC),
                act_start_time=None,
                act_end_time=None,
                nom_start_stop_id="S1",
                nom_end_stop_id="S2",
                nom_total_distance=11.5,
                act_total_distance=None,
            )
            stop_times = [
                StopTimeRecord(
                    operation_day_date=date(2026, 5, 24),
                    trip_id="trip-1",
                    stop_id="S1",
                    distance_from_start=0.0,
                    nom_arrival_time=datetime(2026, 5, 24, 8, 0, tzinfo=UTC),
                    nom_departure_time=datetime(2026, 5, 24, 8, 1, tzinfo=UTC),
                    act_arrival_time=None,
                    act_departure_time=None,
                ),
                StopTimeRecord(
                    operation_day_date=date(2026, 5, 24),
                    trip_id="trip-1",
                    stop_id="S2",
                    distance_from_start=10.0,
                    nom_arrival_time=datetime(2026, 5, 24, 8, 40, tzinfo=UTC),
                    nom_departure_time=datetime(2026, 5, 24, 8, 41, tzinfo=UTC),
                    act_arrival_time=None,
                    act_departure_time=None,
                ),
            ]

            service = MappingService()
            service.register_pipeline_mapping(instance_id="demo", pipeline=pipeline)

            mapped_trip, mapped_stop_times = await service.map_records_for_loading(
                instance_id="demo",
                pipeline_id="nominal",
                trip=trip,
                stop_times=stop_times,
            )

            self.assertEqual("ROUTE-1", mapped_trip.route_id)
            self.assertEqual("STOP-A", mapped_trip.nom_start_stop_id)
            self.assertEqual("STOP-B", mapped_trip.nom_end_stop_id)
            self.assertEqual("STOP-A", mapped_stop_times[0].stop_id)
            self.assertEqual("STOP-B", mapped_stop_times[1].stop_id)


if __name__ == "__main__":
    unittest.main()

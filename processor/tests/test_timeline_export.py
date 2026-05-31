from __future__ import annotations

import io
import unittest
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

try:
    from . import _test_bootstrap
except ImportError:
    import _test_bootstrap

from processor.exports.models import (
    ExportDataSet,
    ExportRouteRow,
    ExportStopRow,
    ExportStopTimeRow,
    ExportTripRow,
)
from processor.exports.timeline_export import TimelineExport
from processor.runtime_config import (
    ExportConfig,
    ExportPeriodConfig,
    ExportProcessingConfig,
    InstanceConfig,
    PipelineConfig,
)

_UTC = timezone.utc

_STOP_TIME = ExportStopTimeRow(
    operation_day_date=date(2026, 5, 30),
    trip_id="trip-1",
    stop_id="stop-A",
    stop_sequence=1,
    distance_from_start=0.0,
    nom_arrival_time=datetime(2026, 5, 30, 8, 0, tzinfo=_UTC),
    nom_departure_time=datetime(2026, 5, 30, 8, 1, tzinfo=_UTC),
    act_arrival_time=None,
    act_departure_time=None,
    schedule_relationship="SCHEDULED",
)

_TRIP = ExportTripRow(
    operation_day_date=date(2026, 5, 30),
    trip_id="trip-1",
    route_id="route-1",
    concessionaire_id=None,
    concessionaire_name=None,
    operator_id="op-1",
    operator_name="Operator One",
    nom_start_time=datetime(2026, 5, 30, 8, 0, tzinfo=_UTC),
    nom_end_time=datetime(2026, 5, 30, 9, 0, tzinfo=_UTC),
    act_start_time=None,
    act_end_time=None,
    nom_start_stop_id="stop-A",
    nom_end_stop_id="stop-B",
    nom_total_distance=10.0,
    act_total_distance=None,
    schedule_relationship="SCHEDULED",
)

_STOP = ExportStopRow(stop_id="stop-A", stop_name="Stop A", stop_lat=48.0, stop_lon=11.0)
_ROUTE = ExportRouteRow(
    route_id="route-1",
    route_name="Route One",
    concessionaire_id=None,
    concessionaire_name=None,
    operator_id="op-1",
    operator_name="Operator One",
)


class RecordingExportRepository:
    def __init__(self, dataset: ExportDataSet) -> None:
        self._dataset = dataset
        self.calls: list[tuple[str, date, date]] = []

    async def get_export_dataset(
        self,
        instance_id: str,
        from_date: date,
        to_date: date,
    ) -> ExportDataSet:
        self.calls.append((instance_id, from_date, to_date))
        return self._dataset


def _make_export_config(
    export_id: str = "daily-trip-export",
    from_day: int = -1,
    to_day: int = 0,
    directory: Path | None = None,
) -> ExportConfig:
    return ExportConfig(
        id=export_id,
        name="timeline-export",
        cron="0 3 * * *",
        period=ExportPeriodConfig(from_day=from_day, to_day=to_day),
        processing=ExportProcessingConfig(directory=directory),
    )


def _make_instance(export: ExportConfig) -> InstanceConfig:
    pipeline = PipelineConfig(
        id="nominal-main",
        name="gtfs",
        type="nominal",
        cron="0 2 * * *",
        endpoint="https://example.test/schedule",
    )
    return InstanceConfig(id="demo", pipelines=(pipeline,), exports=(export,))


class TimelineExportTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_called_with_correct_date_range(self) -> None:
        dataset = ExportDataSet(stop_times=[], trips=[], stops=[], routes=[])
        repository = RecordingExportRepository(dataset)
        export = _make_export_config(from_day=-1, to_day=0)
        instance = _make_instance(export)
        export_obj = TimelineExport(repository=repository)

        current_date = date(2026, 5, 31)
        await export_obj.execute(instance=instance, export=export, current_date=current_date)

        self.assertEqual(len(repository.calls), 1)
        instance_id, from_date, to_date = repository.calls[0]
        self.assertEqual(instance_id, "demo")
        self.assertEqual(from_date, date(2026, 5, 30))
        self.assertEqual(to_date, date(2026, 5, 31))

    async def test_zip_contains_all_four_txt_files(self, tmp_path: Path | None = None) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = Path(tmp_dir)
            dataset = ExportDataSet(
                stop_times=[_STOP_TIME],
                trips=[_TRIP],
                stops=[_STOP],
                routes=[_ROUTE],
            )
            repository = RecordingExportRepository(dataset)
            export = _make_export_config(from_day=-1, to_day=0, directory=directory)
            instance = _make_instance(export)
            export_obj = TimelineExport(repository=repository)

            current_date = date(2026, 5, 31)
            await export_obj.execute(instance=instance, export=export, current_date=current_date)

            zip_files = list(directory.glob("*.zip"))
            self.assertEqual(len(zip_files), 1)

            with zipfile.ZipFile(zip_files[0]) as zf:
                names = set(zf.namelist())
                self.assertIn("stop_times.txt", names)
                self.assertIn("trips.txt", names)
                self.assertIn("stops.txt", names)
                self.assertIn("routes.txt", names)

    async def test_zip_filename_contains_date_range(self) -> None:
        import re
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = Path(tmp_dir)
            dataset = ExportDataSet(stop_times=[], trips=[], stops=[], routes=[])
            repository = RecordingExportRepository(dataset)
            export = _make_export_config(
                export_id="daily-trip-export",
                from_day=-1,
                to_day=0,
                directory=directory,
            )
            instance = _make_instance(export)
            export_obj = TimelineExport(repository=repository)

            await export_obj.execute(
                instance=instance, export=export, current_date=date(2026, 5, 31)
            )

            zip_files = list(directory.glob("*.zip"))
            self.assertEqual(len(zip_files), 1)
            name = zip_files[0].name
            self.assertIn("2026-05-30", name)
            self.assertIn("2026-05-31", name)
            # UUID appended before the extension, separated by "-"
            uuid_pattern = re.compile(
                r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.zip$"
            )
            self.assertRegex(name, uuid_pattern)

    async def test_stop_times_csv_has_correct_header_and_row(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = Path(tmp_dir)
            dataset = ExportDataSet(
                stop_times=[_STOP_TIME],
                trips=[],
                stops=[],
                routes=[],
            )
            repository = RecordingExportRepository(dataset)
            export = _make_export_config(directory=directory)
            instance = _make_instance(export)
            export_obj = TimelineExport(repository=repository)

            await export_obj.execute(
                instance=instance, export=export, current_date=date(2026, 5, 31)
            )

            zip_files = list(directory.glob("*.zip"))
            with zipfile.ZipFile(zip_files[0]) as zf:
                content = zf.read("stop_times.txt").decode("utf-8")

            lines = content.strip().splitlines()
            self.assertEqual(
                lines[0],
                "operation_day_date,trip_id,stop_id,stop_sequence,distance_from_start,"
                "nom_arrival_time,nom_departure_time,act_arrival_time,act_departure_time,"
                "schedule_relationship",
            )
            self.assertIn("trip-1", lines[1])
            self.assertIn("stop-A", lines[1])
            self.assertIn("SCHEDULED", lines[1])

    async def test_nullable_fields_written_as_empty_string(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = Path(tmp_dir)
            dataset = ExportDataSet(
                stop_times=[_STOP_TIME],
                trips=[],
                stops=[],
                routes=[],
            )
            repository = RecordingExportRepository(dataset)
            export = _make_export_config(directory=directory)
            instance = _make_instance(export)
            export_obj = TimelineExport(repository=repository)

            await export_obj.execute(
                instance=instance, export=export, current_date=date(2026, 5, 31)
            )

            zip_files = list(directory.glob("*.zip"))
            with zipfile.ZipFile(zip_files[0]) as zf:
                content = zf.read("stop_times.txt").decode("utf-8")

            # act_arrival_time and act_departure_time are None → empty fields
            data_line = content.strip().splitlines()[1]
            fields = data_line.split(",")
            # act_arrival_time is index 7, act_departure_time is index 8
            self.assertEqual(fields[7], "")
            self.assertEqual(fields[8], "")


if __name__ == "__main__":
    unittest.main()

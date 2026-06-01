from __future__ import annotations

import asyncio
import csv
import io
import uuid
import zipfile
from datetime import date, timedelta
from pathlib import Path

import structlog

from ..runtime_config import ExportConfig, InstanceConfig
from .base_export import ExportBase
from .intf_export_repository import ExportRepositoryInterface
from .models import ExportDataSet, ExportRouteRow, ExportStopRow, ExportStopTimeRow, ExportTripRow

LOGGER = structlog.get_logger(__name__)

_DEFAULT_EXPORT_DIRECTORY = Path("/exports")

_STOP_TIME_COLUMNS = [
    "operation_day_date",
    "trip_id",
    "stop_id",
    "stop_sequence",
    "distance_from_start",
    "nom_arrival_time",
    "nom_departure_time",
    "act_arrival_time",
    "act_departure_time",
    "schedule_relationship",
]

_TRIP_COLUMNS = [
    "operation_day_date",
    "trip_id",
    "route_id",
    "concessionaire_id",
    "concessionaire_name",
    "operator_id",
    "operator_name",
    "nom_start_time",
    "nom_end_time",
    "act_start_time",
    "act_end_time",
    "nom_start_stop_id",
    "nom_end_stop_id",
    "nom_total_distance",
    "act_total_distance",
    "schedule_relationship",
]

_STOP_COLUMNS = [
    "stop_id",
    "stop_name",
    "stop_lat",
    "stop_lon",
]

_ROUTE_COLUMNS = [
    "route_id",
    "route_name",
    "concessionaire_id",
    "concessionaire_name",
    "operator_id",
    "operator_name",
]


def _format_value(value: object) -> str:
    if value is None:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()  # type: ignore[union-attr]
    return str(value)


def _build_csv(headers: list[str], rows: list[list[object]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([_format_value(v) for v in row])
    return buffer.getvalue().encode("utf-8")


def _stop_time_to_row(r: ExportStopTimeRow) -> list[object]:
    return [
        r.operation_day_date,
        r.trip_id,
        r.stop_id,
        r.stop_sequence,
        r.distance_from_start,
        r.nom_arrival_time,
        r.nom_departure_time,
        r.act_arrival_time,
        r.act_departure_time,
        r.schedule_relationship,
    ]


def _trip_to_row(r: ExportTripRow) -> list[object]:
    return [
        r.operation_day_date,
        r.trip_id,
        r.route_id,
        r.concessionaire_id,
        r.concessionaire_name,
        r.operator_id,
        r.operator_name,
        r.nom_start_time,
        r.nom_end_time,
        r.act_start_time,
        r.act_end_time,
        r.nom_start_stop_id,
        r.nom_end_stop_id,
        r.nom_total_distance,
        r.act_total_distance,
        r.schedule_relationship,
    ]


def _stop_to_row(r: ExportStopRow) -> list[object]:
    return [r.stop_id, r.stop_name, r.stop_lat, r.stop_lon]


def _route_to_row(r: ExportRouteRow) -> list[object]:
    return [
        r.route_id,
        r.route_name,
        r.concessionaire_id,
        r.concessionaire_name,
        r.operator_id,
        r.operator_name,
    ]


def _build_zip(dataset: ExportDataSet) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "stop_times.txt",
            _build_csv(_STOP_TIME_COLUMNS, [_stop_time_to_row(r) for r in dataset.stop_times]),
        )
        zf.writestr(
            "trips.txt",
            _build_csv(_TRIP_COLUMNS, [_trip_to_row(r) for r in dataset.trips]),
        )
        zf.writestr(
            "stops.txt",
            _build_csv(_STOP_COLUMNS, [_stop_to_row(r) for r in dataset.stops]),
        )
        zf.writestr(
            "routes.txt",
            _build_csv(_ROUTE_COLUMNS, [_route_to_row(r) for r in dataset.routes]),
        )
    return buffer.getvalue()


class TimelineExport(ExportBase):
    """Produces a ZIP archive of the timeline database for a configurable operation day period."""

    def __init__(self, repository: ExportRepositoryInterface) -> None:
        self._repository = repository

    async def execute(
        self,
        instance: InstanceConfig,
        export: ExportConfig,
        current_date: date,
    ) -> None:
        from_date = current_date + timedelta(days=export.period.from_day)
        to_date = current_date + timedelta(days=export.period.to_day)
        run_id = uuid.uuid4()

        log = LOGGER.bind(
            instance_id=instance.id,
            export_id=export.id,
            run_id=str(run_id),
            from_date=from_date.isoformat(),
            to_date=to_date.isoformat(),
        )

        log.info("timeline_export_fetching_data")
        dataset = await self._repository.get_export_dataset(
            instance_id=instance.id,
            from_date=from_date,
            to_date=to_date,
        )

        zip_bytes = await asyncio.to_thread(_build_zip, dataset)

        directory = export.processing.directory or _DEFAULT_EXPORT_DIRECTORY
        await asyncio.to_thread(
            self._write_zip, directory, export.name, run_id, zip_bytes
        )

        log.info(
            "timeline_export_written",
            stop_time_count=len(dataset.stop_times),
            trip_count=len(dataset.trips),
            stop_count=len(dataset.stops),
            route_count=len(dataset.routes),
        )

    @staticmethod
    def _write_zip(
        directory: Path,
        export_name: str,
        run_id: uuid.UUID,
        zip_bytes: bytes,
    ) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        filename = f"{export_name}-{run_id}.zip"
        output_path = directory / filename
        output_path.write_bytes(zip_bytes)

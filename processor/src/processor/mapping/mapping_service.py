from __future__ import annotations

import csv
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path

from ..loading.models import StopRecord, StopTimeRecord, TripRecord
from ..runtime_config import PipelineConfig
from .intf_mapping_service import MappingServiceInterface


@dataclass(frozen=True, slots=True)
class _PipelineMappingData:
    routes: tuple[tuple[str, str], ...]
    stops: tuple[tuple[str, str], ...]


class MappingServiceError(ValueError):
    """Raised when mapping service usage is invalid."""


class MappingService(MappingServiceInterface):
    def __init__(self) -> None:
        self._pipeline_mapping: dict[tuple[str, str], _PipelineMappingData] = {}

    def register_pipeline_mapping(self, instance_id: str, pipeline: PipelineConfig) -> None:
        routes: tuple[tuple[str, str], ...] = ()
        stops: tuple[tuple[str, str], ...] = ()

        if pipeline.mapping is not None:
            if pipeline.mapping.routes is not None:
                routes = self._read_mapping_csv(pipeline.mapping.routes)
            if pipeline.mapping.stops is not None:
                stops = self._read_mapping_csv(pipeline.mapping.stops)

        self._pipeline_mapping[(instance_id, pipeline.id)] = _PipelineMappingData(
            routes=routes,
            stops=stops,
        )

    async def map_route_id(self, instance_id: str, pipeline_id: str, route_id: str) -> str:
        mapping_data = self._get_mapping_data(instance_id, pipeline_id)
        return self._map_value(route_id, mapping_data.routes)

    async def map_stop_id(self, instance_id: str, pipeline_id: str, stop_id: str) -> str:
        mapping_data = self._get_mapping_data(instance_id, pipeline_id)
        return self._map_value(stop_id, mapping_data.stops)

    async def map_stop_records(
        self,
        instance_id: str,
        pipeline_id: str,
        stops: list[StopRecord],
    ) -> list[StopRecord]:
        mapping_data = self._get_mapping_data(instance_id, pipeline_id)
        return [
            StopRecord(
                stop_id=self._map_value(stop.stop_id, mapping_data.stops),
                stop_name=stop.stop_name,
                stop_lat=stop.stop_lat,
                stop_lon=stop.stop_lon,
            )
            for stop in stops
        ]

    async def map_records_for_loading(
        self,
        instance_id: str,
        pipeline_id: str,
        trip: TripRecord,
        stop_times: list[StopTimeRecord],
    ) -> tuple[TripRecord, list[StopTimeRecord]]:
        mapping_data = self._get_mapping_data(instance_id, pipeline_id)

        mapped_trip = TripRecord(
            operation_day_date=trip.operation_day_date,
            trip_id=trip.trip_id,
            route_id=self._map_value(trip.route_id, mapping_data.routes),
            route_name=trip.route_name,
            concessionaire_id=trip.concessionaire_id,
            concessionaire_name=trip.concessionaire_name,
            operator_id=trip.operator_id,
            operator_name=trip.operator_name,
            nom_start_time=trip.nom_start_time,
            nom_end_time=trip.nom_end_time,
            act_start_time=trip.act_start_time,
            act_end_time=trip.act_end_time,
            nom_start_stop_id=self._map_value(trip.nom_start_stop_id, mapping_data.stops),
            nom_end_stop_id=self._map_value(trip.nom_end_stop_id, mapping_data.stops),
            nom_total_distance=trip.nom_total_distance,
            act_total_distance=trip.act_total_distance,
            schedule_relationship=trip.schedule_relationship,
        )

        mapped_stop_times = [
            StopTimeRecord(
                operation_day_date=record.operation_day_date,
                trip_id=record.trip_id,
                stop_id=self._map_value(record.stop_id, mapping_data.stops),
                distance_from_start=record.distance_from_start,
                nom_arrival_time=record.nom_arrival_time,
                nom_departure_time=record.nom_departure_time,
                act_arrival_time=record.act_arrival_time,
                act_departure_time=record.act_departure_time,
                schedule_relationship=record.schedule_relationship,
                stop_sequence=record.stop_sequence,
                arrival_delay_seconds=record.arrival_delay_seconds,
                departure_delay_seconds=record.departure_delay_seconds,
            )
            for record in stop_times
        ]

        return mapped_trip, mapped_stop_times

    def _get_mapping_data(self, instance_id: str, pipeline_id: str) -> _PipelineMappingData:
        mapping_key = (instance_id, pipeline_id)
        mapping_data = self._pipeline_mapping.get(mapping_key)
        if mapping_data is None:
            raise MappingServiceError(
                f"No mapping registration found for instance '{instance_id}' pipeline '{pipeline_id}'."
            )
        return mapping_data

    def _map_value(self, value: str, mapping_entries: tuple[tuple[str, str], ...]) -> str:
        for key, mapped_value in mapping_entries:
            if key == value:
                return mapped_value

        for key, mapped_value in mapping_entries:
            if "*" in key and fnmatchcase(value, key):
                return mapped_value

        return value

    def _read_mapping_csv(self, csv_path: Path) -> tuple[tuple[str, str], ...]:
        with csv_path.open("r", encoding="utf-8", newline="") as file_obj:
            reader = csv.DictReader(file_obj)
            rows: list[tuple[str, str]] = []
            for row in reader:
                raw_key = (row.get("key") or "").strip()
                raw_value = (row.get("value") or "").strip()
                if not raw_key:
                    continue
                rows.append((raw_key, raw_value))
        return tuple(rows)

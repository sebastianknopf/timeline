from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class ExportStopTimeRow:
    operation_day_date: date
    trip_id: str
    stop_id: str
    stop_sequence: int
    distance_from_start: float
    nom_arrival_time: datetime
    nom_departure_time: datetime
    act_arrival_time: datetime | None
    act_departure_time: datetime | None
    schedule_relationship: str


@dataclass(frozen=True, slots=True)
class ExportTripRow:
    operation_day_date: date
    trip_id: str
    route_id: str
    concessionaire_id: str | None
    concessionaire_name: str | None
    operator_id: str | None
    operator_name: str | None
    nom_start_time: datetime
    nom_end_time: datetime
    act_start_time: datetime | None
    act_end_time: datetime | None
    nom_start_stop_id: str
    nom_end_stop_id: str
    nom_total_distance: float
    act_total_distance: float | None
    schedule_relationship: str


@dataclass(frozen=True, slots=True)
class ExportStopRow:
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float


@dataclass(frozen=True, slots=True)
class ExportRouteRow:
    route_id: str
    route_name: str
    concessionaire_id: str | None
    concessionaire_name: str | None
    operator_id: str | None
    operator_name: str | None


@dataclass(frozen=True, slots=True)
class ExportDataSet:
    stop_times: list[ExportStopTimeRow]
    trips: list[ExportTripRow]
    stops: list[ExportStopRow]
    routes: list[ExportRouteRow]

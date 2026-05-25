from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class StopRecord:
    stop_id: str
    stop_name: str
    stop_lat: float
    stop_lon: float


@dataclass(frozen=True, slots=True)
class TripRecord:
    operation_day_date: date
    trip_id: str
    route_id: str
    route_name: str
    concessionaire_id: str
    concessionaire_name: str
    operator_id: str | None
    operator_name: str | None
    # The following fields are derived from nominal schedule data and resolved by the
    # central load service.  Pipelines that do not have access to nominal DB data must
    # leave these as None; the load service fills them in before persistence.
    nom_start_time: datetime | None = None
    nom_end_time: datetime | None = None
    act_start_time: datetime | None = None
    act_end_time: datetime | None = None
    nom_start_stop_id: str | None = None
    nom_end_stop_id: str | None = None
    nom_total_distance: float | None = None
    act_total_distance: float | None = None
    schedule_relationship: str = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class StopTimeRecord:
    operation_day_date: date
    trip_id: str
    stop_id: str
    distance_from_start: float
    nom_arrival_time: datetime
    nom_departure_time: datetime
    act_arrival_time: datetime | None
    act_departure_time: datetime | None
    schedule_relationship: str = "UNKNOWN"
    stop_sequence: int = 0
    arrival_delay_seconds: int | None = None
    departure_delay_seconds: int | None = None

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
class RouteRecord:
    route_id: str
    route_name: str
    concessionaire_id: str | None
    concessionaire_name: str | None
    operator_id: str | None
    operator_name: str | None


@dataclass(frozen=True, slots=True)
class TripRecord:
    operation_day_date: date
    trip_id: str
    route_id: str
    operator_id: str | None
    operator_name: str | None
    # concessionaire_id and concessionaire_name are stored at route level (dim_routes).
    # Both nominal and realtime pipelines leave them as None at trip level.
    concessionaire_id: str | None = None
    concessionaire_name: str | None = None
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
    realtime_assignment_method: str | None = None
    realtime_pipeline_id: str | None = None
    # Optional: realtime delivered departure and arrival time. Only used for matching
    # when the primary trip ID lookup in the loading service fails to find nominal data.
    # Those fields are NEVER written into the database!
    # Prefix _t_ is used for 'temporary' field.
    _t_scheduled_start_time: datetime | None = None
    _t_scheduled_end_time: datetime | None = None
    _t_scheduled_start_stop_id: str | None = None
    _t_scheduled_end_stop_id: str | None = None
    _t_is_complete_stop_sequence: bool = False


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


@dataclass(frozen=True, slots=True)
class IssueTypeRecord:
    issue_type_id: int
    code: str


@dataclass(frozen=True, slots=True)
class RequestRecord:
    instance_id: str
    request_id: str
    pipeline_id: str
    timestamp: datetime
    num_entities: int
    loaded_direct_trip_count: int = 0
    loaded_matched_trip_count: int = 0
    age_seconds: int = 0
    status_code: int = 200


@dataclass(frozen=True, slots=True)
class QualityIssueRecord:
    instance_id: str
    issue_id: str
    pipeline_id: str
    timestamp: datetime
    entity_id: str
    issue_type_id: int
    concessionaire_id: str | None = None
    concessionaire_name: str | None = None
    operator_id: str | None = None
    operator_name: str | None = None
    assessment_value: str | None = None
    num_affected_values: int = 1

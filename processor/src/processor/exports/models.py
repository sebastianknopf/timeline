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
    arrival_delay_seconds: int | None = None
    departure_delay_seconds: int | None = None


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
    realtime_assignment_method: str | None = None
    realtime_pipeline_id: str | None = None


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
class ExportIssueTypeRow:
    issue_type_id: int
    code: str


@dataclass(frozen=True, slots=True)
class ExportRequestRow:
    request_id: str
    pipeline_id: str
    timestamp: datetime
    num_entities: int
    age_seconds: int
    status_code: int = 200
    loaded_direct_trip_count: int = 0
    loaded_matched_trip_count: int = 0


@dataclass(frozen=True, slots=True)
class ExportQualityIssueRow:
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


@dataclass(frozen=True, slots=True)
class ExportDataSet:
    stop_times: list[ExportStopTimeRow] | None = None
    trips: list[ExportTripRow] | None = None
    stops: list[ExportStopRow] | None = None
    routes: list[ExportRouteRow] | None = None
    issue_types: list[ExportIssueTypeRow] | None = None
    requests: list[ExportRequestRow] | None = None
    quality_issues: list[ExportQualityIssueRow] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "stop_times", self.stop_times or [])
        object.__setattr__(self, "trips", self.trips or [])
        object.__setattr__(self, "stops", self.stops or [])
        object.__setattr__(self, "routes", self.routes or [])
        object.__setattr__(self, "issue_types", self.issue_types or [])
        object.__setattr__(self, "requests", self.requests or [])
        object.__setattr__(self, "quality_issues", self.quality_issues or [])

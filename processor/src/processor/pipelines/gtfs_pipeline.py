from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from io import BytesIO, TextIOWrapper
from itertools import islice
from typing import Iterable, Iterator
from urllib.request import Request, urlopen
import base64
import zipfile
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import structlog

from ..loading.loading_service import LoadingService
from ..loading.models import RouteRecord, StopRecord, StopTimeRecord, TripRecord
from ..mapping.intf_mapping_service import MappingServiceInterface
from ..runtime_config import AuthenticationConfig, InstanceConfig, PipelineConfig

LOGGER = structlog.get_logger(__name__)


REQUIRED_GTFS_FILES: frozenset[str] = frozenset({"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"})


@dataclass(frozen=True, slots=True)
class _AgencyInfo:
    agency_id: str
    agency_name: str
    timezone_name: str


@dataclass(frozen=True, slots=True)
class _RouteInfo:
    route_id: str
    route_name: str
    concessionaire_id: str
    concessionaire_name: str


@dataclass(frozen=True, slots=True)
class _TripMeta:
    trip_id: str
    route_id: str
    shape_id: str


@dataclass(slots=True)
class _ShapeIndexState:
    """Mutable per-shape accumulation state used while streaming shapes.txt."""

    dist_traveled: float | None = None
    coord_km: float = 0.0
    prev_lat: float | None = None
    prev_lon: float | None = None
    prev_seq: int | None = None


@dataclass(frozen=True, slots=True)
class _StopTimeRow:
    stop_id: str
    stop_sequence: int
    arrival_time_raw: str
    departure_time_raw: str
    shape_dist_traveled_raw: str


class GtfsPipelineError(RuntimeError):
    """Raised when GTFS nominal processing cannot produce a valid load payload."""


class GtfsNominalPipeline:
    def __init__(
        self,
        loading_service: LoadingService,
        mapping_service: MappingServiceInterface,
        processor_timezone_name: str = "UTC",
    ) -> None:
        self._loading_service = loading_service
        self._mapping_service = mapping_service
        self._processor_timezone_name = processor_timezone_name
        self._processor_timezone = _safe_zoneinfo(processor_timezone_name)

    async def execute(self, instance: InstanceConfig, pipeline: PipelineConfig) -> None:
        if pipeline.name != "gtfs":
            raise GtfsPipelineError(
                f"GtfsNominalPipeline cannot execute pipeline '{pipeline.id}' with name '{pipeline.name}'."
            )

        feed = _GtfsArchive.open_endpoint(
            endpoint=pipeline.endpoint,
            authentication=pipeline.authentication,
        )

        try:
            agencies = self._read_agencies(feed=feed)
            source_timezone_name = _resolve_agency_timezone_name(
                agencies=agencies,
                fallback_timezone_name=self._processor_timezone_name,
            )
            source_timezone = _safe_zoneinfo(source_timezone_name)
            operation_day = datetime.now(self._processor_timezone).date()
            valid_service_ids = self._resolve_valid_service_ids(feed=feed, operation_day=operation_day)
            routes = self._read_routes(feed=feed, agencies=agencies, pipeline=pipeline)
            trips = self._read_trips(feed=feed, valid_service_ids=valid_service_ids)
            shape_index = self._read_shape_index(feed=feed)
            stop_times_by_trip = self._read_stop_times(feed=feed, valid_trip_ids=set(trips.keys()))
            stop_records = self._read_stops(feed=feed, referenced_stop_ids=_referenced_stop_ids(stop_times_by_trip))
        finally:
            feed.close()

        if not trips:
            LOGGER.info("gtfs_no_trips_for_operation_day", instance_id=instance.id, pipeline_id=pipeline.id)
            return

        mapped_stops = await self._mapping_service.map_stop_records(
            instance_id=instance.id,
            pipeline_id=pipeline.id,
            stops=stop_records,
        )
        unique_mapped_stops = _deduplicate_stop_records(mapped_stops)

        route_records: dict[str, RouteRecord] = {}
        trip_records: list[TripRecord] = []
        stop_time_records: list[StopTimeRecord] = []

        for trip_id, trip_meta in trips.items():
            route = routes.get(trip_meta.route_id)
            if route is None:
                continue

            source_rows = stop_times_by_trip.get(trip_id, [])
            transformed_rows = self._transform_stop_times(
                operation_day=operation_day,
                source_timezone=source_timezone,
                target_timezone=self._processor_timezone,
                trip_id=trip_id,
                rows=source_rows,
            )
            if not transformed_rows:
                continue

            trip_record = self._build_trip_record(
                operation_day=operation_day,
                trip_id=trip_id,
                route=route,
                stop_times=transformed_rows,
                shape_id=trip_meta.shape_id,
                shape_index=shape_index,
            )
            mapped_trip, mapped_stop_times = await self._mapping_service.map_records_for_loading(
                instance_id=instance.id,
                pipeline_id=pipeline.id,
                trip=trip_record,
                stop_times=transformed_rows,
            )
            trip_records.append(mapped_trip)
            stop_time_records.extend(mapped_stop_times)

            if mapped_trip.route_id not in route_records:
                route_records[mapped_trip.route_id] = self._build_route_record(
                    route=route,
                    mapped_route_id=mapped_trip.route_id,
                )

        if not trip_records:
            LOGGER.info("gtfs_no_valid_trip_payload", instance_id=instance.id, pipeline_id=pipeline.id)
            return

        unique_mapped_stops = _ensure_required_stops_present(
            stops=unique_mapped_stops,
            trips=trip_records,
            stop_times=stop_time_records,
        )

        for stops_chunk in _chunked(unique_mapped_stops, 5000):
            await self._loading_service.load_nominal_stops_batch(instance_id=instance.id, stops=stops_chunk)

        route_record_list = list(route_records.values())
        for routes_chunk in _chunked(route_record_list, 5000):
            await self._loading_service.load_nominal_routes_batch(instance_id=instance.id, routes=routes_chunk)

        for trips_chunk in _chunked(trip_records, 2000):
            await self._loading_service.load_nominal_trips_batch(instance_id=instance.id, trips=trips_chunk)

        for stop_times_chunk in _chunked(stop_time_records, 20000):
            await self._loading_service.load_nominal_stop_times_batch(
                instance_id=instance.id,
                stop_times=stop_times_chunk,
            )

        LOGGER.info(
            "gtfs_nominal_pipeline_loaded",
            instance_id=instance.id,
            pipeline_id=pipeline.id,
            operation_day=str(operation_day),
            processor_timezone=self._processor_timezone_name,
            source_timezone=source_timezone_name,
            stop_count=len(unique_mapped_stops),
            route_count=len(route_record_list),
            trip_count=len(trip_records),
            stop_time_count=len(stop_time_records),
            shape_index_count=len(shape_index),
        )

    def _resolve_valid_service_ids(self, feed: "_GtfsArchive", operation_day: date) -> set[str]:
        has_calendar = feed.has_file("calendar.txt")
        has_calendar_dates = feed.has_file("calendar_dates.txt")
        if not has_calendar and not has_calendar_dates:
            raise GtfsPipelineError("GTFS feed must include calendar.txt and/or calendar_dates.txt.")

        services: set[str] = set()

        if has_calendar:
            weekday_field = {
                0: "monday",
                1: "tuesday",
                2: "wednesday",
                3: "thursday",
                4: "friday",
                5: "saturday",
                6: "sunday",
            }[operation_day.weekday()]
            for row in feed.iter_csv_rows("calendar.txt"):
                service_id = (row.get("service_id") or "").strip()
                if not service_id:
                    continue

                start_date = _parse_gtfs_calendar_date((row.get("start_date") or "").strip())
                end_date = _parse_gtfs_calendar_date((row.get("end_date") or "").strip())
                if start_date is None or end_date is None:
                    continue
                if not (start_date <= operation_day <= end_date):
                    continue
                if (row.get(weekday_field) or "").strip() != "1":
                    continue
                services.add(service_id)

        if has_calendar_dates:
            for row in feed.iter_csv_rows("calendar_dates.txt"):
                service_id = (row.get("service_id") or "").strip()
                exception_date = _parse_gtfs_calendar_date((row.get("date") or "").strip())
                exception_type = (row.get("exception_type") or "").strip()
                if not service_id or exception_date != operation_day:
                    continue

                if exception_type == "1":
                    services.add(service_id)
                elif exception_type == "2":
                    services.discard(service_id)

        return services

    def _read_agencies(self, feed: "_GtfsArchive") -> dict[str, _AgencyInfo]:
        if not feed.has_file("agency.txt"):
            return {}

        rows = list(feed.iter_csv_rows("agency.txt"))
        if not rows:
            return {}

        agencies: dict[str, _AgencyInfo] = {}
        for index, row in enumerate(rows):
            agency_id = (row.get("agency_id") or "").strip()
            fallback_agency_id = f"agency-{index + 1}"
            resolved_agency_id = agency_id or fallback_agency_id

            agencies[resolved_agency_id] = _AgencyInfo(
                agency_id=resolved_agency_id,
                agency_name=(row.get("agency_name") or "").strip() or resolved_agency_id,
                timezone_name=(row.get("agency_timezone") or "").strip(),
            )

        return agencies

    def _read_routes(
        self,
        feed: "_GtfsArchive",
        agencies: dict[str, _AgencyInfo],
        pipeline: PipelineConfig,
    ) -> dict[str, _RouteInfo]:
        if not feed.has_file("routes.txt"):
            raise GtfsPipelineError("GTFS feed is missing required file routes.txt.")

        fallback_agency_id = _parameter_as_str(
            parameters=pipeline.parameters,
            key="fallback_agency_id",
            default="UNKNOWN-AGENCY",
        )
        fallback_agency_name = _parameter_as_str(
            parameters=pipeline.parameters,
            key="fallback_agency_name",
            default="Unknown Agency",
        )

        default_agency = next(iter(agencies.values()), None)

        routes: dict[str, _RouteInfo] = {}
        for row in feed.iter_csv_rows("routes.txt"):
            route_id = (row.get("route_id") or "").strip()
            if not route_id:
                continue

            route_name = (
                (row.get("route_short_name") or "").strip()
                or (row.get("route_long_name") or "").strip()
                or route_id
            )

            agency_id = (row.get("agency_id") or "").strip()
            agency = agencies.get(agency_id) if agency_id else default_agency
            concessionaire_id = agency.agency_id if agency else fallback_agency_id
            concessionaire_name = agency.agency_name if agency else fallback_agency_name

            routes[route_id] = _RouteInfo(
                route_id=route_id,
                route_name=route_name,
                concessionaire_id=concessionaire_id,
                concessionaire_name=concessionaire_name,
            )

        return routes

    def _read_trips(
        self,
        feed: "_GtfsArchive",
        valid_service_ids: set[str],
    ) -> dict[str, _TripMeta]:
        if not feed.has_file("trips.txt"):
            raise GtfsPipelineError("GTFS feed is missing required file trips.txt.")

        trips: dict[str, _TripMeta] = {}
        for row in feed.iter_csv_rows("trips.txt"):
            trip_id = (row.get("trip_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            service_id = (row.get("service_id") or "").strip()

            if not trip_id or not route_id or not service_id:
                continue
            if service_id not in valid_service_ids:
                continue

            trips[trip_id] = _TripMeta(
                trip_id=trip_id,
                route_id=route_id,
                shape_id=(row.get("shape_id") or "").strip(),
            )

        return trips

    def _read_shape_index(self, feed: "_GtfsArchive") -> dict[str, float]:
        """Build a shape_id → total-distance-in-km index by streaming shapes.txt.

        Two complementary strategies are applied in a single streaming pass:

        1. ``shape_dist_traveled`` strategy: if the column is present and
           non-empty for a shape point, the running maximum per shape is tracked.
           The maximum of all ``shape_dist_traveled`` values equals the total
           path length (last point has the highest cumulative value).  The same
           meter-detection heuristic used for stop_times applies: if the maximum
           exceeds 200 the values are treated as meters and divided by 1000.

        2. Coordinate strategy: if ``shape_dist_traveled`` is absent or empty for
           a shape, the cumulative Haversine distance between consecutive shape
           points (in ``shape_pt_sequence`` order) is accumulated.  Memory usage
           is O(number of distinct shape_ids) – only the last seen point per shape
           is retained, not the full point list.

        The ``shape_dist_traveled`` strategy takes precedence when any non-zero
        value is available for a shape.
        """
        if not feed.has_file("shapes.txt"):
            return {}

        states: dict[str, _ShapeIndexState] = {}

        for row in feed.iter_csv_rows("shapes.txt"):
            shape_id = (row.get("shape_id") or "").strip()
            if not shape_id:
                continue

            state = states.get(shape_id)
            if state is None:
                state = _ShapeIndexState()
                states[shape_id] = state

            # --- strategy 1: shape_dist_traveled ---
            raw_dist = (row.get("shape_dist_traveled") or "").strip()
            if raw_dist:
                try:
                    dist_val = float(raw_dist)
                except ValueError:
                    dist_val = None
                if dist_val is not None and dist_val >= 0:
                    state.dist_traveled = max(state.dist_traveled or 0.0, dist_val)

            # --- strategy 2: coordinate accumulation ---
            raw_seq = (row.get("shape_pt_sequence") or "").strip()
            raw_lat = (row.get("shape_pt_lat") or "").strip()
            raw_lon = (row.get("shape_pt_lon") or "").strip()
            seq = _parse_int(raw_seq)
            try:
                lat = float(raw_lat) if raw_lat else None
                lon = float(raw_lon) if raw_lon else None
            except ValueError:
                lat, lon = None, None

            if seq is not None and lat is not None and lon is not None:
                if (
                    state.prev_lat is not None
                    and state.prev_lon is not None
                    and state.prev_seq is not None
                    and seq > state.prev_seq
                ):
                    state.coord_km += _haversine_km(state.prev_lat, state.prev_lon, lat, lon)
                state.prev_lat = lat
                state.prev_lon = lon
                state.prev_seq = seq

        # Resolve final distance per shape, preferring shape_dist_traveled.
        index: dict[str, float] = {}
        for shape_id, state in states.items():
            if state.dist_traveled is not None and state.dist_traveled > 0:
                # Apply the same meter-detection heuristic as for stop_times.
                raw = state.dist_traveled
                dist_km = raw / 1000.0 if raw > 200.0 else raw
            else:
                dist_km = state.coord_km

            if dist_km > 0:
                index[shape_id] = dist_km

        return index

    def _read_stop_times(
        self,
        feed: "_GtfsArchive",
        valid_trip_ids: set[str],
    ) -> dict[str, list[_StopTimeRow]]:
        if not feed.has_file("stop_times.txt"):
            raise GtfsPipelineError("GTFS feed is missing required file stop_times.txt.")

        rows_by_trip: dict[str, list[_StopTimeRow]] = {}
        for row in feed.iter_csv_rows("stop_times.txt"):
            trip_id = (row.get("trip_id") or "").strip()
            if trip_id not in valid_trip_ids:
                continue

            stop_id = (row.get("stop_id") or "").strip()
            stop_sequence = _parse_int((row.get("stop_sequence") or "").strip())
            if not stop_id or stop_sequence is None:
                continue

            rows_by_trip.setdefault(trip_id, []).append(
                _StopTimeRow(
                    stop_id=stop_id,
                    stop_sequence=stop_sequence,
                    arrival_time_raw=(row.get("arrival_time") or "").strip(),
                    departure_time_raw=(row.get("departure_time") or "").strip(),
                    shape_dist_traveled_raw=(row.get("shape_dist_traveled") or "").strip(),
                )
            )

        for rows in rows_by_trip.values():
            rows.sort(key=lambda row: row.stop_sequence)

        return rows_by_trip

    def _read_stops(
        self,
        feed: "_GtfsArchive",
        referenced_stop_ids: set[str],
    ) -> list[StopRecord]:
        if not feed.has_file("stops.txt"):
            raise GtfsPipelineError("GTFS feed is missing required file stops.txt.")

        stops: list[StopRecord] = []
        for row in feed.iter_csv_rows("stops.txt"):
            stop_id = (row.get("stop_id") or "").strip()
            if not stop_id:
                continue

            location_type = (row.get("location_type") or "").strip()
            if location_type not in {"", "0"} and stop_id not in referenced_stop_ids:
                continue

            stops.append(
                StopRecord(
                    stop_id=stop_id,
                    stop_name=(row.get("stop_name") or "").strip() or stop_id,
                    stop_lat=_parse_float((row.get("stop_lat") or "").strip(), default=0.0),
                    stop_lon=_parse_float((row.get("stop_lon") or "").strip(), default=0.0),
                )
            )

        return stops

    def _transform_stop_times(
        self,
        operation_day: date,
        source_timezone: ZoneInfo,
        target_timezone: ZoneInfo,
        trip_id: str,
        rows: list[_StopTimeRow],
    ) -> list[StopTimeRecord]:
        transformed: list[StopTimeRecord] = []
        distance_km_by_sequence = _normalize_shape_distance_values_km(rows)

        for row in rows:
            arrival = _parse_gtfs_time(
                raw_value=row.arrival_time_raw,
                operation_day=operation_day,
                source_timezone=source_timezone,
                target_timezone=target_timezone,
            )
            departure = _parse_gtfs_time(
                raw_value=row.departure_time_raw,
                operation_day=operation_day,
                source_timezone=source_timezone,
                target_timezone=target_timezone,
            )
            if arrival is None and departure is None:
                LOGGER.warning(
                    "gtfs_stop_time_row_rejected",
                    trip_id=trip_id,
                    stop_id=row.stop_id,
                    stop_sequence=row.stop_sequence,
                    reason="arrival_time_and_departure_time_missing",
                )
                continue

            if arrival is None:
                arrival = departure
            if departure is None:
                departure = arrival

            distance = distance_km_by_sequence.get(row.stop_sequence, 0.0)
            transformed.append(
                StopTimeRecord(
                    operation_day_date=operation_day,
                    trip_id=trip_id,
                    stop_id=row.stop_id,
                    distance_from_start=distance,
                    nom_arrival_time=arrival,
                    nom_departure_time=departure,
                    act_arrival_time=None,
                    act_departure_time=None,
                    schedule_relationship="UNKNOWN",
                    stop_sequence=row.stop_sequence,
                )
            )

        return transformed

    def _build_route_record(self, route: _RouteInfo, mapped_route_id: str) -> RouteRecord:
        return RouteRecord(
            route_id=mapped_route_id,
            route_name=route.route_name,
            concessionaire_id=route.concessionaire_id,
            concessionaire_name=route.concessionaire_name,
            operator_id=None,
            operator_name=None,
        )

    def _build_trip_record(
        self,
        operation_day: date,
        trip_id: str,
        route: _RouteInfo,
        stop_times: list[StopTimeRecord],
        shape_id: str = "",
        shape_index: dict[str, float] | None = None,
    ) -> TripRecord:
        first_stop = stop_times[0]
        last_stop = stop_times[-1]

        # Primary source: maximum distance_from_start derived from shape_dist_traveled
        # in stop_times.txt (already normalized to km by _transform_stop_times).
        nom_total_distance = max(item.distance_from_start for item in stop_times)

        # Fallback: if stop_times did not carry useful distance information, look up
        # the pre-built shape index by the trip's shape_id.
        if nom_total_distance == 0.0 and shape_id and shape_index:
            nom_total_distance = shape_index.get(shape_id, 0.0)

        return TripRecord(
            operation_day_date=operation_day,
            trip_id=trip_id,
            route_id=route.route_id,
            # concessionaire_* and operator_* are stored at route level (dim_routes).
            operator_id=None,
            operator_name=None,
            nom_start_time=first_stop.nom_arrival_time,
            nom_end_time=last_stop.nom_departure_time,
            act_start_time=None,
            act_end_time=None,
            nom_start_stop_id=first_stop.stop_id,
            nom_end_stop_id=last_stop.stop_id,
            nom_total_distance=nom_total_distance,
            act_total_distance=None,
            schedule_relationship="UNKNOWN",
        )


class _GtfsArchive:
    def __init__(self, archive: zipfile.ZipFile, buffer: BytesIO) -> None:
        self._archive = archive
        self._buffer = buffer
        self._names = set(archive.namelist())

        missing_required_files = REQUIRED_GTFS_FILES - self._names
        if missing_required_files:
            missing = ", ".join(sorted(missing_required_files))
            raise GtfsPipelineError(f"GTFS feed is missing required files: {missing}.")

    @classmethod
    def open_endpoint(
        cls,
        endpoint: str,
        authentication: AuthenticationConfig | None,
    ) -> "_GtfsArchive":
        request = Request(endpoint)
        for key, value in _build_auth_headers(authentication).items():
            request.add_header(key, value)

        with urlopen(request, timeout=120) as response:
            buffer = BytesIO()
            while True:
                chunk = response.read(65536)
                if not chunk:
                    break
                buffer.write(chunk)

        buffer.seek(0)
        try:
            archive = zipfile.ZipFile(buffer)
        except zipfile.BadZipFile as exc:
            raise GtfsPipelineError("GTFS endpoint did not return a valid ZIP archive.") from exc

        return cls(archive=archive, buffer=buffer)

    def has_file(self, filename: str) -> bool:
        return filename in self._names

    def iter_csv_rows(self, filename: str) -> Iterator[dict[str, str]]:
        with self._archive.open(filename, "r") as file_obj:
            with TextIOWrapper(file_obj, encoding="utf-8-sig", newline="") as text_stream:
                reader = csv.DictReader(text_stream)
                for row in reader:
                    yield {key: value or "" for key, value in row.items()}

    def close(self) -> None:
        self._archive.close()
        self._buffer.close()


def _chunked(items: list[StopRecord] | list[RouteRecord] | list[TripRecord] | list[StopTimeRecord], size: int) -> Iterator[list[object]]:
    iterator = iter(items)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


def _build_auth_headers(authentication: AuthenticationConfig | None) -> dict[str, str]:
    if authentication is None:
        return {}

    if authentication.token:
        return {"Authorization": f"Bearer {authentication.token}"}

    if authentication.username and authentication.password:
        raw = f"{authentication.username}:{authentication.password}".encode("utf-8")
        encoded = base64.b64encode(raw).decode("ascii")
        return {"Authorization": f"Basic {encoded}"}

    return {}


def _safe_zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        LOGGER.warning("gtfs_invalid_timezone_fallback", timezone=timezone_name)
        return ZoneInfo("UTC")


def _resolve_agency_timezone_name(
    agencies: dict[str, _AgencyInfo],
    fallback_timezone_name: str,
) -> str:
    if not agencies:
        return fallback_timezone_name

    first_agency = next(iter(agencies.values()))
    return first_agency.timezone_name or fallback_timezone_name


def _parse_gtfs_calendar_date(raw_value: str) -> date | None:
    if not raw_value:
        return None
    try:
        return datetime.strptime(raw_value, "%Y%m%d").date()
    except ValueError:
        return None


def _parse_gtfs_time(
    raw_value: str,
    operation_day: date,
    source_timezone: ZoneInfo,
    target_timezone: ZoneInfo,
) -> datetime | None:
    if not raw_value:
        return None

    parts = raw_value.split(":")
    if len(parts) != 3:
        return None

    hours = _parse_int(parts[0])
    minutes = _parse_int(parts[1])
    seconds = _parse_int(parts[2])
    if hours is None or minutes is None or seconds is None:
        return None
    if minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60 or hours < 0:
        return None

    base = datetime.combine(operation_day, time(0, 0), tzinfo=source_timezone)
    source_timestamp = base + timedelta(hours=hours, minutes=minutes, seconds=seconds)
    return source_timestamp.astimezone(target_timezone)


def _referenced_stop_ids(stop_times_by_trip: dict[str, list[_StopTimeRow]]) -> set[str]:
    referenced: set[str] = set()
    for rows in stop_times_by_trip.values():
        for row in rows:
            referenced.add(row.stop_id)
    return referenced


def _deduplicate_stop_records(stops: list[StopRecord]) -> list[StopRecord]:
    by_id: dict[str, StopRecord] = {}
    for stop in stops:
        if stop.stop_id not in by_id:
            by_id[stop.stop_id] = stop
    return list(by_id.values())


def _ensure_required_stops_present(
    stops: list[StopRecord],
    trips: list[TripRecord],
    stop_times: list[StopTimeRecord],
) -> list[StopRecord]:
    by_id: dict[str, StopRecord] = {stop.stop_id: stop for stop in stops}
    referenced_stop_ids: set[str] = set()

    for trip in trips:
        referenced_stop_ids.add(trip.nom_start_stop_id)
        referenced_stop_ids.add(trip.nom_end_stop_id)

    for stop_time in stop_times:
        referenced_stop_ids.add(stop_time.stop_id)

    missing_stop_ids = sorted(stop_id for stop_id in referenced_stop_ids if stop_id and stop_id not in by_id)
    if not missing_stop_ids:
        return list(by_id.values())

    LOGGER.warning(
        "gtfs_missing_stop_dimension_rows_created",
        missing_count=len(missing_stop_ids),
        sample_stop_ids=missing_stop_ids[:20],
    )

    for stop_id in missing_stop_ids:
        by_id[stop_id] = StopRecord(
            stop_id=stop_id,
            stop_name=stop_id,
            stop_lat=0.0,
            stop_lon=0.0,
        )

    return list(by_id.values())


def _parameter_as_str(parameters: dict[str, object], key: str, default: str) -> str:
    raw = parameters.get(key)
    if raw is None:
        return default
    value = str(raw).strip()
    return value or default


def _parse_int(raw_value: str) -> int | None:
    try:
        return int(raw_value)
    except ValueError:
        return None


def _normalize_shape_distance_values_km(rows: list[_StopTimeRow]) -> dict[int, float]:
    parsed_by_sequence: dict[int, float | None] = {}
    positive_values: list[float] = []

    for row in rows:
        parsed = _parse_float(row.shape_dist_traveled_raw, default=None)
        if parsed is None or parsed < 0:
            parsed_by_sequence[row.stop_sequence] = None
            continue

        parsed_by_sequence[row.stop_sequence] = parsed
        if parsed > 0:
            positive_values.append(parsed)

    # Heuristic: very large values are typically meters in GTFS, small values are usually kilometers.
    is_meter_scale = bool(positive_values) and max(positive_values) > 200.0

    normalized: dict[int, float] = {}
    for row in rows:
        raw_value = parsed_by_sequence.get(row.stop_sequence)
        if raw_value is None:
            normalized[row.stop_sequence] = 0.0
            continue

        distance_km = raw_value / 1000.0 if is_meter_scale else raw_value
        if distance_km < 0 or distance_km > 1000:
            LOGGER.warning(
                "gtfs_shape_distance_unplausible",
                stop_sequence=row.stop_sequence,
                raw_value=raw_value,
                normalized_km=distance_km,
            )
            normalized[row.stop_sequence] = 0.0
            continue

        normalized[row.stop_sequence] = distance_km

    return normalized


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in kilometres using the Haversine formula."""
    r = 6371.0  # Earth mean radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * r * math.asin(math.sqrt(a))


def _parse_float(raw_value: str, default: float) -> float:
    try:
        return float(raw_value)
    except ValueError:
        return default

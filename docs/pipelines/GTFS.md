# Pipeline Definition: GTFS

## Title

General Transit Feed Specification Pipeline

## Name

`gtfs`

This value is used in configuration as `pipeline.name`.

## Description

GTFS is the nominal-data pipeline definition for scheduled service data.

This pipeline is expected to run on low-frequency schedules, typically once per day.

The pipeline fetches source data, transforms it into the normalized Timeline model, and hands the normalized output to the central load service.

Reference: Official GTFS Static Schedule specification at https://gtfs.org/documentation/schedule/reference/.

## Shared Configuration Parameters

This pipeline uses these shared configuration keys:

- `id`
- `name`
- `type` (must be `nominal` for this pipeline)
- `cron`
- `endpoint`
- `authentication` (optional)
- `filter` (optional)
     - `operators`
     - `routes`

## Optional Pipeline Parameters

The following optional parameters are currently defined for this pipeline:

- `fallback_agency_id`: fallback value used for `concessionaire_id` when GTFS route/agency linkage is missing
- `fallback_agency_name`: fallback value used for `concessionaire_name` when GTFS agency name resolution is missing

## Filtering
During the execution, the pipeline respects the optional `filter` parameters. Filter types (include/exclude) are respeced. If at least one filter is set, only data matching those filters are imported. Agencies are filtered against their ID based on `filter.operators[...].match`. Routes are filtered against their ID based on `filter.routes[...].match`. The filters reduce all underlaying data (routes, trips, stop times, stops) if set. If no `filter` is set at all, the whole feed will be imported.

## Assumptions

The following assumptions were made while mapping GTFS static data into the Timeline model. They should be reviewed and approved explicitly.

| ID | Assumption | Reason / Impact |
| --- | --- | --- |
| A1 | The feed is standard GTFS static ZIP with files at ZIP root (not in a nested directory). | Required for deterministic streaming extraction and parser discovery. |
| A2 | `stops.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, and at least one service-calendar source (`calendar.txt` and/or `calendar_dates.txt`) are present and parseable. | These files are required to produce complete `dim_stops`, `dim_trips`, and `fact_stop_times`. |
| A3 | Timeline `instance_id` is injected from pipeline runtime context (configuration instance), not from GTFS. | GTFS has no tenant/instance concept. |
| A4 | `operation_day_date` is derived from the current calendar date in `PROCESSOR_TIMEZONE` at pipeline runtime. UTC is never used for this derivation. For example, when the processor runs at 00:30 Europe/Berlin (UTC+2), the resolved date is the Berlin date — not the UTC date, which would be 22:30 the previous day. This boundary behaviour is covered by a dedicated unit test. A future pipeline version may apply a configured day offset to this base date. | Prevents day-boundary errors when `PROCESSOR_TIMEZONE` is ahead of UTC: using UTC would shift `operation_day_date` into the previous calendar day for operators in UTC+ zones running post-midnight schedules. Service-day selection and stop-time anchoring must always reflect the local operational date. |
| A5 | GTFS times in `stop_times.arrival_time` and `stop_times.departure_time` are first interpreted in `agency.agency_timezone` and then converted to `PROCESSOR_TIMEZONE` before loading. Values over 24:00:00 are rolled to the next calendar day(s). | Matches GTFS source semantics while normalizing all Timeline timestamps to one configured timezone. |
| A15 | If `agency.txt` is missing or no usable `agency_timezone` is available, the pipeline falls back to `PROCESSOR_TIMEZONE` as source timezone for GTFS time parsing. | Provides deterministic timezone behavior for feeds without agency timezone metadata. |
| A6 | `dim_trips.nom_start_time`/`nom_end_time` come from first/last ordered stop_time timestamps for a trip on a service day. | Needed because GTFS does not provide explicit trip start/end timestamp fields. |
| A7 | `dim_trips.nom_start_stop_id`/`nom_end_stop_id` come from the first/last ordered `stop_times.stop_id` per trip/service day. | Required by non-null constraints in Timeline schema. |
| A8 | `route_name` in Timeline is resolved as `routes.route_short_name` if present, otherwise `routes.route_long_name`, otherwise `route_id`. This value is stored in `dim_routes.route_name`. | GTFS allows either short or long name; Timeline requires a non-null route_name at route level. |
| A9 | `concessionaire_id` is mapped from GTFS `agency_id`; `concessionaire_name` from `agency_name`. If resolution fails, fallback values are taken from pipeline parameters `fallback_agency_id` and `fallback_agency_name`. These values are stored in `dim_routes`, **not** `dim_trips`. | Timeline requires non-null concession fields at route level; GTFS may omit `agency_id` in single-agency feeds. |
| A10 | GTFS static does not provide operator ownership in this model scope, therefore `dim_routes.operator_id` and `dim_routes.operator_name` are always set to null. `dim_trips.operator_id` and `dim_trips.operator_name` are also always null for this pipeline. | Keeps semantics explicit and avoids pseudo-operator values. |
| A11 | `schedule_relationship` for nominal records defaults to `UNKNOWN` unless a deterministic static classification rule is added later. | GTFS static does not expose realtime-like schedule relationship values. |
| A12 | `distance_from_start` uses `stop_times.shape_dist_traveled` normalized to kilometers. If values appear meter-based they are converted (`m -> km`). Missing, negative, or unplausible values default to `0.0`. | Timeline requires non-null distance in fact rows and consistent distance units. |
| A13 | If `stop_times.arrival_time` or `departure_time` is missing for intermediate stops, the missing value is filled from the available counterpart in that row. If both are missing, the row is rejected and reported. | Timeline requires non-null nominal timestamps in `fact_stop_times`. |
| A14 | `nom_total_distance` is populated per imported trip as follows: (1) primary source is the greatest resolved `distance_from_start` value derived from `stop_times.shape_dist_traveled`; (2) if that yields `0.0`, the shape index (built from `shapes.txt`) is consulted by the trip's `shape_id` — see A17; (3) if neither source provides a usable value, `nom_total_distance` remains `0.0`. `act_total_distance` is always null in this nominal pipeline. | Keeps distance aggregates consistent with nominal-only ingestion scope while providing a useful fallback for feeds that carry shapes but omit `shape_dist_traveled` in stop_times. |
| A16 | If a trip references a stop id that is missing from `stops.txt` (or removed by mapping output mismatch), the pipeline creates a placeholder `dim_stops` row (`stop_name=stop_id`, coordinates `0.0/0.0`) before writing trips and stop-times. | Guarantees foreign-key integrity for `dim_trips.nom_start_stop_id` / `nom_end_stop_id` and `fact_stop_times.stop_id` without aborting the full import run. |
| A17 | If `shapes.txt` is present in the GTFS feed, the pipeline builds a **shape index** (`shape_id → total_distance_km`) in a single streaming pass over the file (memory O(number of distinct shape_ids)). Two strategies are applied: (a) if `shape_dist_traveled` is present for a shape, the maximum value across all points is used as total distance; (b) otherwise, the cumulative Haversine distance between consecutive shape points (in `shape_pt_sequence` order) is accumulated. The meter-detection heuristic (value > 200 → divide by 1000) is applied to `shape_dist_traveled` from shapes.txt as well. The resulting index is used as a fallback for `nom_total_distance` when `stop_times.shape_dist_traveled` is absent or yields `0.0`. | Enables useful trip-level distance data for feeds that provide shapes but omit `shape_dist_traveled` in stop_times. |

## Transformations

| Step | Input | Transformation | Output |
| --- | --- | --- | --- |
| T1 | GTFS ZIP stream from `endpoint` | Stream download to temporary file or stream buffer with checksum/size validation and hard fail on incomplete archive. | Validated ZIP artifact |
| T2 | ZIP entries | Stream enumerate root-level `.txt` files and build file manifest. | File availability manifest |
| T3 | `calendar.txt`, `calendar_dates.txt` | Extract all valid `service_id` values for the current calendar date (`operation_day_date`) derived in `PROCESSOR_TIMEZONE`, using `calendar` rules with `calendar_dates` additions/removals, or from the single available file if only one exists. | `valid_service_ids_for_today` |
| T4 | `agency.txt` | Build agency lookup (`agency_id`, `agency_name`, timezone). | Agency lookup index |
| T5 | `routes.txt` | Normalize route names and attach agency/concession metadata with fallback for missing `agency_id`. | Route lookup index |
| T6 | `stops.txt` + referenced `stop_times.stop_id` | Use location_type 0 or empty as primary rule, but if a trip references a stop with another location type, still import that referenced stop to preserve referential completeness. If stop coordinates are missing or invalid, set `stop_lat` and `stop_lon` to `0.0` and keep the row. If a referenced stop id is still missing after parsing/mapping, synthesize a placeholder stop row with `stop_name=stop_id` and `0.0/0.0` coordinates. | `dim_stops` candidate rows |
| T7 | `trips.txt` | Join trips to routes and keep only trips whose `service_id` is in `valid_service_ids_for_today`. | Current-day trip metadata stream |
| T8 | `stop_times.txt` + timezone context | Group by trip_id in stop_sequence order; extract `stop_sequence` into Timeline facts; parse GTFS time strings in agency timezone (fallback `PROCESSOR_TIMEZONE`), including values >24:00:00, then convert parsed timestamps to `PROCESSOR_TIMEZONE`. Normalize `shape_dist_traveled` to kilometers with meter-detection heuristic and fallback defaults. If `shapes.txt` is present, also build a shape index (`shape_id → total_distance_km`) in a single streaming pass (see A17); this index is used in T9 as a distance fallback. | Ordered trip stop-time stream in processor timezone |
| T9 | Current-day trip metadata + ordered stop_times + shape index | Compute nominal trip aggregates: start/end timestamps, start/end stops, `nom_total_distance` (primary: max `shape_dist_traveled` from stop_times; fallback: shape index lookup by `trips.shape_id` — see A17), and `schedule_relationship`. Also set `act_total_distance`, `concessionaire_id`, `concessionaire_name`, `operator_id`, `operator_name` to null at trip level. Build unique `dim_routes` records keyed by mapped `route_id`, capturing `route_name`, `concessionaire_id`, `concessionaire_name` from the route source; `operator_id` and `operator_name` are null for this pipeline. | `dim_trips` nominal rows, `dim_routes` rows |
| T10 | Current-day trip metadata + ordered stop_times | Emit per-stop fact rows with resolved nominal timestamps, distance_from_start, and schedule_relationship for the same current day. | `fact_stop_times` nominal rows |
| T11 | Nominal outputs | Set all `act_*` fields to null for nominal pipeline output objects. | Timeline-compatible nominal model objects |
| T12 | All output entities | Deduplicate by natural keys and validate non-null/type constraints against Timeline schema contract before load handoff. | Validated load payload |

## Mappings

| Timeline Entity | Timeline Field | Source (GTFS) | Mapping Rule |
| --- | --- | --- | --- |
| `dim_stops` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_stops` | `stop_id` | `stops.stop_id` | Direct mapping. |
| `dim_stops` | `stop_name` | `stops.stop_name` | Direct mapping; trim whitespace. |
| `dim_stops` | `stop_lat` | `stops.stop_lat` | Parse to `double precision`; if missing/unparseable set to `0.0`. |
| `dim_stops` | `stop_lon` | `stops.stop_lon` | Parse to `double precision`; if missing/unparseable set to `0.0`. |
| `dim_routes` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_routes` | `route_id` | `trips.route_id` (after mapping) | Mapped route_id from the trip's mapped record. |
| `dim_routes` | `route_name` | `routes.route_short_name` / `routes.route_long_name` / `routes.route_id` | Prefer short name, else long name, else route_id fallback (see A8). |
| `dim_routes` | `concessionaire_id` | `routes.agency_id` or parameter fallback | Map from route agency; fallback to `parameters.fallback_agency_id` when missing (see A9). |
| `dim_routes` | `concessionaire_name` | `agency.agency_name` or parameter fallback | Resolve by agency lookup; fallback to `parameters.fallback_agency_name` when missing (see A9). |
| `dim_routes` | `operator_id` | n/a (static feed) | Set `null` (see A10). |
| `dim_routes` | `operator_name` | n/a (static feed) | Set `null` (see A10). |
| `dim_trips` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_trips` | `operation_day_date` | pipeline runtime date | Set to the current calendar date of the pipeline run in `PROCESSOR_TIMEZONE` (future-compatible with configurable day offset). |
| `dim_trips` | `trip_id` | `trips.trip_id` | Direct mapping. |
| `dim_trips` | `route_id` | `trips.route_id` | Direct mapping; references `dim_routes.route_id` via FK. |
| `dim_trips` | `concessionaire_id` | n/a | Set `null`. Route-level concession data is stored in `dim_routes`. |
| `dim_trips` | `concessionaire_name` | n/a | Set `null`. Route-level concession data is stored in `dim_routes`. |
| `dim_trips` | `operator_id` | n/a (static feed) | Set `null`. |
| `dim_trips` | `operator_name` | n/a (static feed) | Set `null`. |
| `dim_trips` | `nom_start_time` | first ordered stop_time per trip | Parse GTFS time using operation_day_date + agency timezone (fallback `PROCESSOR_TIMEZONE`), then convert to `PROCESSOR_TIMEZONE`. |
| `dim_trips` | `nom_end_time` | last ordered stop_time per trip | Parse GTFS time using operation_day_date + agency timezone (fallback `PROCESSOR_TIMEZONE`), then convert to `PROCESSOR_TIMEZONE`. |
| `dim_trips` | `act_start_time` | n/a (static feed) | Set `null`. |
| `dim_trips` | `act_end_time` | n/a (static feed) | Set `null`. |
| `dim_trips` | `nom_start_stop_id` | first ordered `stop_times.stop_id` | Direct mapping from first stop-time record in the imported trip. |
| `dim_trips` | `nom_end_stop_id` | last ordered `stop_times.stop_id` | Direct mapping from last stop-time record in the imported trip. |
| `dim_trips` | `nom_total_distance` | `stop_times.shape_dist_traveled` (primary) or `shapes.shape_dist_traveled` / shape coordinates (fallback via shape index) | Prefer max `shape_dist_traveled` from imported stop_times (normalized to km). If that yields `0.0`, look up the shape index by `trips.shape_id`: use max `shape_dist_traveled` from `shapes.txt` (same meter-detection heuristic applies), or Haversine-accumulated coordinate distance if `shape_dist_traveled` is absent in shapes.txt. Remain `0.0` if neither source is available. See A14 and A17. |
| `dim_trips` | `act_total_distance` | n/a (static feed) | Set `null`. |
| `dim_trips` | `schedule_relationship` | n/a (static feed) | Set default `UNKNOWN`. |
| `fact_stop_times` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `fact_stop_times` | `operation_day_date` | pipeline runtime date | Same current calendar date used for trip import scope in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `trip_id` | `stop_times.trip_id` | Direct mapping. |
| `fact_stop_times` | `stop_id` | `stop_times.stop_id` | Direct mapping. |
| `fact_stop_times` | `stop_sequence` | `stop_times.stop_sequence` | Direct mapping; used for deterministic stop ordering and realtime matching. |
| `fact_stop_times` | `distance_from_start` | `stop_times.shape_dist_traveled` or fallback | Normalize to kilometers. If source seems meter-based, divide by 1000. Missing/unplausible values become `0.0`. Note: `distance_from_start` is derived solely from `stop_times.shape_dist_traveled`; the shape index (A17) is only used for `nom_total_distance` at trip level and does not back-fill per-stop distances. Because `fact_stop_times` is keyed by `stop_sequence` (not `distance_from_start`), corrected or updated distance values from later pipeline runs are applied via upsert to existing rows, preventing stale distance accumulation. |
| `fact_stop_times` | `nom_arrival_time` | `stop_times.arrival_time` | Parse GTFS time using service day + agency timezone (fallback `PROCESSOR_TIMEZONE`), convert to `PROCESSOR_TIMEZONE`; if missing use departure_time. |
| `fact_stop_times` | `nom_departure_time` | `stop_times.departure_time` | Parse GTFS time using service day + agency timezone (fallback `PROCESSOR_TIMEZONE`), convert to `PROCESSOR_TIMEZONE`; if missing use arrival_time. |
| `fact_stop_times` | `act_arrival_time` | n/a (static feed) | Set `null`. |
| `fact_stop_times` | `act_departure_time` | n/a (static feed) | Set `null`. |
| `fact_stop_times` | `schedule_relationship` | n/a (static feed) | Set default `UNKNOWN`. |

## Implementation Details

The GTFS pipeline should be implemented to minimize memory usage by using streaming in every extraction phase and only materializing final load objects.

### End-to-end flow

1. Open endpoint stream and download GTFS ZIP as a streamed HTTP response.
2. Iterate ZIP entries in streaming mode and open each required `.txt` file as a stream reader.
3. Parse CSV rows incrementally (row-by-row), never loading full files into memory.
4. Build only compact lookup structures required for joins (for example route lookup, agency lookup, service calendar resolver, shape index).
5. Process `stop_times` in streaming order and construct current-day output batches.
6. Materialize final normalized objects in bounded batches and hand them to the central load service.

### Import scope per run

- One run imports only trips relevant for one service day: the current calendar date at runtime.
- Trip eligibility is determined by valid `service_id` values for that current date.
- Only trips linked to those valid `service_id` values are transformed and loaded.
- No multi-day expansion is performed in this pipeline version.

### Streaming requirements

- Downloading must be streamed from `endpoint`; no full response buffering in memory.
- ZIP extraction must read entry streams directly; no full archive decompression into memory.
- CSV parsing must be iterator-based.
- Transformation stages should use generator/pipeline style processing with bounded batch sizes.
- Backpressure from the central load service should be respected by reducing batch dispatch rate.

### Non-streaming exception

Only the final Timeline model objects passed to the central load service may be handled in non-streaming fashion, and only as bounded batches.

### Suggested batching strategy

- `dim_stops`: upsert in moderate batches (for example 5k-20k rows).
- `dim_trips`: upsert in moderate batches aligned to operation_day partitions.
- `fact_stop_times`: upsert in larger but bounded batches (for example 20k-100k rows) depending on loader throughput.

### Error handling and observability

- Reject malformed rows with structured error records including file name, row number, trip_id/stop_id context when available.
- Fail the pipeline when required file sets or mandatory non-null Timeline fields cannot be produced.
- Emit metrics for rows read, rows rejected, imported trips, and load batches dispatched.
# Pipeline Definition: GTFS-RT Trip Updates

## Title

General Transit Feed Specification Realtime Pipeline

## Name

`gtfsrt-tripupdates`

This value is used in configuration as `pipeline.name`.

## Description

GTFS-RT Trip Updates is the realtime-data pipeline definition for trip update messages.

This pipeline transforms trip updates only.

This pipeline is expected to run at high frequency, typically at least once per minute.

The pipeline fetches source data, transforms it into the normalized Timeline model, and hands the normalized output to the central load service.

Reference: Official GTFS Realtime specification at https://gtfs.org/documentation/realtime/reference/.

## Shared Configuration Parameters

This pipeline uses these shared configuration keys:

- `id`
- `name`
- `type` (must be `realtime` for this pipeline)
- `cron`
- `endpoint`
- `authentication` (optional)

## Optional Pipeline Parameters

Pipeline-specific parameters may be added here in the future.

## Assumptions

The following assumptions were made while mapping GTFS Realtime TripUpdate data into the Timeline model. They should be reviewed and approved explicitly.

| ID | Assumption | Reason / Impact |
| --- | --- | --- |
| A1 | The endpoint returns GTFS Realtime `FeedMessage` protobuf payloads that contain `TripUpdate` entities. | This pipeline processes Trip Updates only. |
| A2 | `operation_day_date` is resolved from `TripDescriptor.start_date` when present; otherwise the current calendar date at runtime in `PROCESSOR_TIMEZONE` is used as fallback. In this realtime pipeline, that resolved `operation_day_date` is the date used for matching and finding the corresponding nominal trip. | Supports late/overlapping realtime updates (for example updates still referencing yesterday service date). |
| A3 | `instance_id` is injected from runtime pipeline instance context. | GTFS Realtime has no native tenant key. |
| A4 | `TripDescriptor` resolves to a single trip instance (typically via `trip_id`, with start_date/start_time for frequency-based ambiguity). | Prevents ambiguous writes across multiple trip instances. |
| A5 | The central load service owns identifier matching when a realtime trip/stop cannot be directly matched to existing nominal data. | Matches repository architecture responsibilities. |
| A6 | `operator_id` and `operator_name` remain null in this pipeline because GTFS-RT TripUpdate does not provide authoritative operator ownership for this model. | Keeps semantics consistent with current database model decisions. |
| A7 | For mutable realtime fields (`act_*`, `schedule_relationship`), the latest valid information seen wins. | Required by business rule for realtime state convergence. |
| A8 | A trip that has entirely run until its last station is considered closed and is no longer transformed or updated by this pipeline. The completion decision is based on realtime status, not on nominal status. | Required by business rule to avoid reopening completed trips and to ensure realtime authority over completion state. |
| A9 | Stop-time updates whose arrival or departure timestamp is earlier than processing time (`now`) are not touched anymore. | Required by business rule to freeze past events. |
| A10 | For nominally unknown trips for the resolved `operation_day_date`, realtime updates may still be accepted and passed to load service for matching/upsert workflows. | Supports delayed nominal ingestion and replacement/new trip scenarios. |

## Transformations

| Step | Input | Transformation | Output |
| --- | --- | --- | --- |
| T1 | GTFS-RT payload stream from `endpoint` | Stream download and decode protobuf `FeedMessage`. | Parsed feed envelope |
| T2 | `FeedHeader` + processing clock | Validate feed freshness and compute `now` in `PROCESSOR_TIMEZONE`; current calendar date in `PROCESSOR_TIMEZONE` is kept as fallback only when `TripDescriptor.start_date` is absent. | Run context |
| T3 | `FeedEntity.trip_update` entries | Keep only entities with `trip_update`; ignore unrelated entity types. | TripUpdate stream |
| T4 | `TripDescriptor` + static/nominal keys | Resolve trip identity and key tuple (`instance_id`, `operation_day_date`, `trip_id`) for model writes, where `operation_day_date` comes from `TripDescriptor.start_date` or runtime-date fallback when absent. | Resolved trip keys |
| T5 | TripUpdate stream | Drop updates for trips already marked as completed (fully run to last station). | Active trip updates only |
| T6 | `StopTimeUpdate` rows | Normalize stop selector (`stop_sequence` preferred for repeated stops, else `stop_id`) and resolve arrival/departure event times with strict priority: absolute `time` first, `delay` only as fallback when `time` is missing. Delay fallback is derived from `TripUpdate.timestamp`, then `FeedHeader.timestamp`, then processing time. | Normalized stop-time update rows |
| T7 | Normalized rows + `now` | Central load service excludes rows where both effective arrival/departure timestamps (absolute event timestamp or delay-as-offset-to-nominal fallback) are earlier than `now`. | Mutable realtime rows |
| T8 | Mutable realtime rows | Apply latest-wins merge for `act_arrival_time`, `act_departure_time`, and `schedule_relationship` using feed/TripUpdate timestamp ordering. | Resolved field updates |
| T9 | Resolved rows | Build `fact_stop_times` upsert payload for resolved `operation_day_date` trip/stop keys. Only mutable realtime fields are modified. | Fact upsert batch |
| T10 | Trip-level updates | Central load service derives trip-level realtime aggregates (`act_start_time`, `act_end_time`, `act_total_distance`) from stop order and nominal boundary rows. | `dim_trips` upsert batch |
| T11 | Trip-level schedule relation | Update `dim_trips.schedule_relationship` from TripDescriptor schedule relationship (latest-wins). | Trip schedule relation updates |
| T12 | Final payloads | Convert all resolved realtime datetimes (including delay-derived values) to `PROCESSOR_TIMEZONE` before upsert. Upsert everything still mutable; do not touch completed trips or frozen past stop updates. | Realtime upsert payload |

## Mappings

| Timeline Entity | Timeline Field | Source (GTFS-RT) | Mapping Rule |
| --- | --- | --- | --- |
| `dim_trips` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_trips` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time in `PROCESSOR_TIMEZONE`. |
| `dim_trips` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `dim_trips` | `act_start_time` | first nominal stop row + realtime data | Uses first nominal stop realtime departure time when available, else first nominal stop realtime arrival time, else first nominal stop departure time (`nom_start_time`). |
| `dim_trips` | `act_end_time` | last nominal stop row + realtime data | Candidate uses last nominal stop realtime arrival time when available, else last nominal stop realtime departure time, else last nominal stop arrival/departure nominal fallback. Persisted only when candidate timestamp is <= current processing timestamp; otherwise `NULL`. |
| `dim_trips` | `act_total_distance` | max mutable stop distance | Derived from mutable stop updates; latest-wins per trip. |
| `dim_trips` | `schedule_relationship` | `TripDescriptor.schedule_relationship` | Latest-wins update from trip-level schedule relationship. |
| `dim_trips` | `operator_id` | n/a | Always `null`. |
| `dim_trips` | `operator_name` | n/a | Always `null`. |
| `fact_stop_times` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `fact_stop_times` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `fact_stop_times` | `stop_id` | `StopTimeUpdate.stop_id` or mapped from `stop_sequence` | Prefer explicit stop_id; otherwise resolve via nominal stop sequence mapping. |
| `fact_stop_times` | `act_arrival_time` | `StopTimeEvent.time` or `StopTimeEvent.delay` | Prefer absolute `time`; if absent, use `delay` as offset to nominal arrival time; persist in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `act_departure_time` | `StopTimeEvent.time` or `StopTimeEvent.delay` | Prefer absolute `time`; if absent, use `delay` as offset to nominal departure time; persist in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `schedule_relationship` | `StopTimeUpdate.schedule_relationship` | Latest-wins update per stop row; default from trip-level if missing and policy applies. |

### StopTimeEvent Timestamp Priority

For each `StopTimeEvent` (`arrival`, `departure`), the resolver uses this strict order:

1. `event.time` (absolute UNIX timestamp) is authoritative and always preferred.
2. `event.delay` is used only when `event.time` is absent.
3. Delay fallback is computed as offset to the nominal stop-time value (arrival/departure respectively).

All resolved timestamps from steps above are converted to `PROCESSOR_TIMEZONE` before filtering and persistence.

### Trip Boundary Time Derivation

- `act_start_time` is always derived from the first stop-time row (stop order), not by taking a global minimum over all stops.
- `act_end_time` is always derived from the last stop-time row (stop order), not by taking a global maximum over all stops.
- If first/last stop realtime values are unavailable, fallback uses nominal boundary times from that same stop-time row (`nom_start_time`/`nom_end_time`).
- `act_end_time` is persisted only when the derived end candidate timestamp is reached (`<= now`); otherwise it is stored as `NULL`.

## Update Rules

These rules are mandatory for realtime writes in this pipeline:

1. Latest-wins for mutable fields:
- `fact_stop_times.act_arrival_time`
- `fact_stop_times.act_departure_time`
- `fact_stop_times.schedule_relationship`
- `dim_trips.act_start_time`
- `dim_trips.act_end_time`
- `dim_trips.act_total_distance`
- `dim_trips.schedule_relationship`

Nominal immutability rule:
- Realtime conflict updates must not modify any `nom_*` columns in `dim_trips` or `fact_stop_times`.
- `nom_*` columns are owned by the GTFS static nominal pipeline.
- Realtime conflict updates also do not modify non-realtime dimension attributes; updates are restricted to `act_*` and `schedule_relationship` fields.

2. Completed-trip freeze:
- If a trip has already run entirely to its last station, the trip is no longer transformed or updated.

3. Past-stop freeze:
- Stop-time updates with arrival/departure timestamps before current processing time are not touched anymore.

4. Remaining mutable rows are upserted:
- Everything that is not frozen by the rules above is upserted.

## Implementation Details

The GTFSRT-TRIPUPDATES pipeline should be implemented in streaming fashion end-to-end, with bounded in-memory state and idempotent upsert behavior.

### End-to-end flow

1. Open endpoint stream and decode protobuf feed message in streaming mode.
2. Iterate TripUpdate entities incrementally.
3. Resolve trip identity and skip closed/completed trips.
4. Iterate stop_time_update rows, applying time-based freeze filtering (`< now`).
5. Build bounded upsert batches for mutable trip-level and stop-level fields.
6. Send batches to central load service; repeat on next scheduler tick.

### Streaming requirements

- No full historical snapshot should be materialized in memory.
- Parsing and transformation should be iterator-based over feed entities.
- Matching indexes and dedup caches should be bounded and short-lived per run.
- Backpressure from load service should throttle batch emission.

### State and idempotency

- Upserts must be idempotent for repeated feed snapshots.
- Latest-wins ordering should use feed header timestamp and/or TripUpdate timestamp as primary freshness hints.
- If timestamps are equal, deterministic tie-breakers (stable entity ordering) should be applied.

### Import scope per run

- Scope is realtime trip updates in the current feed snapshot.
- For each trip update, `operation_day_date` is `TripDescriptor.start_date` when present, otherwise current calendar date in `PROCESSOR_TIMEZONE`.
- Only mutable realtime state is processed.
- Closed trips and frozen past stop events are excluded from transformation.

### Error handling and observability

- Reject unresolved or invalid trip descriptors with structured diagnostics.
- Report counters for: entities received, entities filtered, completed-trip skips, frozen stop-time skips, and successful upserts.
- Log late/out-of-order updates when they lose latest-wins arbitration.
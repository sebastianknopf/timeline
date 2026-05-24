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
| A2 | `operation_day_date` is resolved from `TripDescriptor.start_date` when present; otherwise the current calendar date at runtime is used as fallback. In this realtime pipeline, that resolved `operation_day_date` is the date used for matching and finding the corresponding nominal trip. | Supports late/overlapping realtime updates (for example updates still referencing yesterday service date). |
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
| T2 | `FeedHeader` + processing clock | Validate feed freshness and compute `now`; current calendar date is kept as fallback only when `TripDescriptor.start_date` is absent. | Run context |
| T3 | `FeedEntity.trip_update` entries | Keep only entities with `trip_update`; ignore unrelated entity types. | TripUpdate stream |
| T4 | `TripDescriptor` + static/nominal keys | Resolve trip identity and key tuple (`instance_id`, `operation_day_date`, `trip_id`) for model writes, where `operation_day_date` comes from `TripDescriptor.start_date` or runtime-date fallback when absent. | Resolved trip keys |
| T5 | TripUpdate stream | Drop updates for trips already marked as completed (fully run to last station). | Active trip updates only |
| T6 | `StopTimeUpdate` rows | Normalize stop selector (`stop_sequence` preferred for repeated stops, else `stop_id`) and parse arrival/departure times. | Normalized stop-time update rows |
| T7 | Normalized rows + `now` | Exclude rows where both available event times (arrival/departure) are earlier than `now`. | Mutable realtime rows |
| T8 | Mutable realtime rows | Apply latest-wins merge for `act_arrival_time`, `act_departure_time`, and `schedule_relationship` using feed/TripUpdate timestamp ordering. | Resolved field updates |
| T9 | Resolved rows | Build `fact_stop_times` upsert payload for resolved `operation_day_date` trip/stop keys. Only mutable realtime fields are modified. | Fact upsert batch |
| T10 | Trip-level updates | Derive trip-level realtime aggregates (`act_start_time`, `act_end_time`, `act_total_distance`) from currently mutable stop updates and latest-wins policy. | `dim_trips` upsert batch |
| T11 | Trip-level schedule relation | Update `dim_trips.schedule_relationship` from TripDescriptor schedule relationship (latest-wins). | Trip schedule relation updates |
| T12 | Final payloads | Upsert everything still mutable; do not touch completed trips or frozen past stop updates. | Realtime upsert payload |

## Mappings

| Timeline Entity | Timeline Field | Source (GTFS-RT) | Mapping Rule |
| --- | --- | --- | --- |
| `dim_trips` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_trips` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time. |
| `dim_trips` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `dim_trips` | `act_start_time` | earliest mutable stop actual time | Latest-wins field update; only from non-frozen updates. |
| `dim_trips` | `act_end_time` | latest mutable stop actual time | Latest-wins field update; only from non-frozen updates. |
| `dim_trips` | `act_total_distance` | max mutable stop distance | Derived from mutable stop updates; latest-wins per trip. |
| `dim_trips` | `schedule_relationship` | `TripDescriptor.schedule_relationship` | Latest-wins update from trip-level schedule relationship. |
| `dim_trips` | `operator_id` | n/a | Always `null`. |
| `dim_trips` | `operator_name` | n/a | Always `null`. |
| `fact_stop_times` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `fact_stop_times` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time. |
| `fact_stop_times` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `fact_stop_times` | `stop_id` | `StopTimeUpdate.stop_id` or mapped from `stop_sequence` | Prefer explicit stop_id; otherwise resolve via nominal stop sequence mapping. |
| `fact_stop_times` | `act_arrival_time` | `StopTimeUpdate.arrival.time` | Update only if timestamp is not frozen (>= `now`) and newer than stored value context. |
| `fact_stop_times` | `act_departure_time` | `StopTimeUpdate.departure.time` | Update only if timestamp is not frozen (>= `now`) and newer than stored value context. |
| `fact_stop_times` | `schedule_relationship` | `StopTimeUpdate.schedule_relationship` | Latest-wins update per stop row; default from trip-level if missing and policy applies. |

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
- For each trip update, `operation_day_date` is `TripDescriptor.start_date` when present, otherwise current calendar date.
- Only mutable realtime state is processed.
- Closed trips and frozen past stop events are excluded from transformation.

### Error handling and observability

- Reject unresolved or invalid trip descriptors with structured diagnostics.
- Report counters for: entities received, entities filtered, completed-trip skips, frozen stop-time skips, and successful upserts.
- Log late/out-of-order updates when they lose latest-wins arbitration.
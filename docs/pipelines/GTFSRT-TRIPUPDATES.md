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

## Assumptions

The following assumptions were made while mapping GTFS Realtime TripUpdate data into the Timeline model. They should be reviewed and approved explicitly.

| ID | Assumption | Reason / Impact |
| --- | --- | --- |
| A1 | The endpoint returns GTFS Realtime `FeedMessage` protobuf payloads that contain `TripUpdate` entities. | This pipeline processes Trip Updates only. |
| A2 | `operation_day_date` is resolved from `TripDescriptor.start_date` when present; otherwise the current calendar date at runtime in `PROCESSOR_TIMEZONE` is used as fallback. In this realtime pipeline, that resolved `operation_day_date` is the date used for matching and finding the corresponding nominal trip. | Supports late/overlapping realtime updates (for example updates still referencing yesterday service date). |
| A3 | `instance_id` is injected from runtime pipeline instance context. | GTFS Realtime has no native tenant key. |
| A4 | `TripDescriptor` resolves to a single trip instance (typically via `trip_id`, with start_date/start_time for frequency-based ambiguity). | Prevents ambiguous writes across multiple trip instances. |
| A5 | `LoadingService` owns nominal-baseline matching. When nominal data is loaded for a trip, each realtime stop-time row is matched to the nominal stop by `stop_sequence` inside `LoadingService._apply_nominal_baseline`. Stop-time rows that cannot be matched to a nominal sequence are silently dropped. | Guarantees every persisted stop-time row has correct `nom_arrival_time`, `nom_departure_time`, and `distance_from_start` values derived from the schedule. |
| A6 | `operator_id` and `operator_name` remain null in this pipeline because GTFS-RT TripUpdate does not provide authoritative operator ownership for this model. | Keeps semantics consistent with current database model decisions. |
| A7 | For mutable realtime fields (`act_*`, `schedule_relationship`), the latest valid information seen wins. | Required by business rule for realtime state convergence. |
| A8 | `act_end_time` (and `act_start_time`) is written on every update as long as realtime data is available for at least one stop. The source stop for `act_start_time` is the first entry in `normalized_stop_times` by `stop_sequence`; the source stop for `act_end_time` is the last entry. These may differ from the nominal first/last stops when the nominal boundary stops are absent from the feed (no explicit update and no propagation reaching them). `schedule_relationship` of the boundary stop in `normalized_stop_times` is never consulted for this decision; any schedule relationship is accepted. Neither value is gated on a comparison to the current processing time. | Required by business rule to ensure trip boundary times reflect the latest available realtime estimate from the first update onwards, covering all schedule relationship variants and all feed coverage patterns. |
| A9 | Stop-time updates are processed regardless of whether timestamps are in the past or future. | Required by business rule to keep full trip-update state convergent. |
| A10 | Trips with `schedule_relationship = ADDED` are completely discarded by the pipeline and not forwarded to the load service. Support for ADDED trips and stop times is deferred to a future implementation stage. All other trips that cannot be matched to nominal data (either directly by `trip_id` or alternatively by `route_id` + `start_time`) are also **discarded** by the load service and not persisted. | Prevents phantom trip rows caused by realtime data arriving before the nominal pipeline has run, while keeping the system architecture consistent until ADDED trip support is explicitly designed. |
| A12 | ADDED trips (`schedule_relationship = ADDED`) and ADDED stop times are not supported in this pipeline version and are discarded with a debug log entry. Support will be added in a later implementation stage. |
| A11 | When a `StopTimeUpdate` provides a delay or absolute time correction for a stop but the feed does not include explicit updates for all subsequent stops, `LoadingService._apply_nominal_baseline` propagates the last known effective delay forward to every subsequent nominal stop that has no explicit update. Propagation iterates through the nominal stop sequence in ascending `stop_sequence` order. The effective delay is re-computed at each new explicit update and carried forward again from that point. `schedule_relationship` is always set to `SCHEDULED` for synthesized propagated stops; explicit stop updates retain their own `schedule_relationship`. Stops that precede the first explicit update in the nominal sequence are not affected. This behaviour is verified by `test_realtime_propagates_delay_to_nominal_stops_missing_from_feed` and `test_realtime_propagates_delay_value_to_stops_missing_from_feed` in `processor/tests/test_load_service.py`, and by propagation scenario tests in `processor/tests/test_gtfsrt_tripupdates_pipeline.py`. | Ensures a complete realtime picture of the trip even when the feed only delivers partial stop coverage, matching the GTFS-RT specification propagation rule. `LoadingService` is the correct owner of this logic because only it has access to the full nominal stop baseline. |
| A13 | Routes (`dim_routes`), stops (`dim_stops`), and trips (`dim_trips`) must be pre-loaded by the nominal pipeline before any realtime update is applied. The realtime update for `dim_trips` uses a pure `UPDATE` statement: if no matching row exists, the statement is a silent no-op and no new trip row is created. This prevents phantom rows and eliminates the risk of foreign-key violations against `dim_routes`. | Enforces the nominal-first invariant at the database layer without requiring explicit pre-flight checks in the pipeline or load service. |
| A14 | `act_total_distance` is derived from `nom_total_distance` using a binary rule based on `schedule_relationship`. `SCHEDULED` trips receive `nom_total_distance` as `act_total_distance`. All other relationships (`UNKNOWN`, `CANCELLED`, …) receive `0.0`. **This is an all-or-nothing model**: a trip that was only partially operated is currently attributed the same distance as a fully operated (`SCHEDULED`) or a fully cancelled trip. Per-segment partial-distance accounting is not supported in this version. The `nom_total_distance` used for this derivation is read from the **stored nominal `TripRecord`** in `dim_trips` (including values derived from the shape index by the nominal pipeline). A stop-level max of `distance_from_start` across all nominal stop times is used as fallback only when no stored nominal trip is found. | Provides a deterministic, consistent distance attribution for reporting without requiring per-stop actual-distance data. Using the stored trip-level distance avoids returning `act_total_distance = 0.0` for SCHEDULED trips in GTFS feeds that lack per-stop `shape_dist_traveled` but do provide shape geometry. |

## Transformations

| Step | Input | Transformation | Output |
| --- | --- | --- | --- |
| T1 | GTFS-RT payload stream from `endpoint` | Stream download and decode protobuf `FeedMessage`. | Parsed feed envelope |
| T2 | `FeedHeader` + processing clock | Validate feed freshness and compute `now` in `PROCESSOR_TIMEZONE`; current calendar date in `PROCESSOR_TIMEZONE` is kept as fallback only when `TripDescriptor.start_date` is absent. | Run context |
| T3 | `FeedEntity.trip_update` entries | Keep only entities with `trip_update`; ignore unrelated entity types. | TripUpdate stream |
| T4 | `TripDescriptor` + static/nominal keys | Resolve trip identity and key tuple (`instance_id`, `operation_day_date`, `trip_id`) for model writes, where `operation_day_date` comes from `TripDescriptor.start_date` or runtime-date fallback when absent. | Resolved trip keys |
| T5 | TripUpdate stream | Drop updates for trips already marked as completed (fully run to last station). | Active trip updates only |
| T6 | `StopTimeUpdate` rows | Normalize stop selector (`stop_sequence` preferred, `stop_id` secondary) and resolve event values with strict priority: absolute `time` first, `delay` only as fallback when `time` is missing. Delay fallback is calculated as offset to nominal stop-time values (arrival/departure respectively) in central load service. | Normalized stop-time update rows |
| T6a | Normalized stop rows | If only one realtime side exists for a stop (`act_arrival_time` or `act_departure_time`), mirror it to the missing side before trip-boundary aggregation. | Synchronized realtime stop rows |
| T6b | Explicit stop updates + nominal stop sequence | For each nominal stop that has no explicit `StopTimeUpdate`, check whether a preceding explicit update exists. If yes, synthesize a propagated `StopTimeRecord` using the tracked effective delay (arrival and departure handled separately) applied to the nominal baseline times for that stop. `schedule_relationship` is set to `SCHEDULED` for all synthesized records. Stops before the first explicit update remain untouched. The effective delay at each explicit stop is derived as: `round((act_arrival_time − nom_arrival_time).total_seconds())` when an absolute time is present; `arrival_delay_seconds` otherwise (same rule for departure). Each new explicit update resets the tracked delay for subsequent propagation from that point. | Fully propagated realtime stop-time stream including all nominal stops |
| T7 | Normalized rows | Central load service keeps all rows after timestamp resolution (absolute or delay-as-offset-to-nominal fallback), independent of temporal position relative to `now`. | Mutable realtime rows |
| T8 | Mutable realtime rows | Apply latest-wins merge for `act_arrival_time`, `act_departure_time`, and `schedule_relationship` using feed/TripUpdate timestamp ordering. | Resolved field updates |
| T9 | Resolved rows | Build `fact_stop_times` upsert payload for resolved `operation_day_date` trip/stop keys. Only mutable realtime fields are modified. | Fact upsert batch |
| T10 | Trip-level updates | Central load service derives trip-level realtime aggregates (`act_start_time`, `act_end_time`, `act_total_distance`) from the **first and last entries in `normalized_stop_times`** (sorted by `stop_sequence`). `act_start_time` uses `act_departure_time` of the first normalized stop (falls back to `nom_departure_time`). `act_end_time` uses `act_arrival_time` of the last normalized stop, falling back to `act_departure_time` and then to `nom_departure_time`. Both values are written unconditionally on every update, regardless of `schedule_relationship` and regardless of whether the timestamp is in the past or future. `nom_total_distance` is resolved from the **stored nominal `TripRecord`** fetched via `get_nominal_trip` (which reflects the value stored by the nominal pipeline, including any shape-index derivation). When the stored nominal trip is unavailable, `nom_total_distance` falls back to `max(distance_from_start)` across all nominal stop times. `act_total_distance` is then derived from the resolved `nom_total_distance` based on `schedule_relationship`: for `SCHEDULED` trips `act_total_distance` is set to `nom_total_distance`; for all other relationships (`UNKNOWN`, `CANCELLED`, …) it is set to `0.0`. | `dim_trips` upsert batch |
| T11 | Trip-level schedule relation | Update `dim_trips.schedule_relationship` from TripDescriptor schedule relationship (latest-wins). | Trip schedule relation updates |
| T12 | Final payloads | Convert all resolved realtime datetimes (including delay-derived values) to `PROCESSOR_TIMEZONE` before upsert. Upsert everything still mutable; do not touch completed trips. | Realtime upsert payload |

## Mappings

| Timeline Entity | Timeline Field | Source (GTFS-RT) | Mapping Rule |
| --- | --- | --- | --- |
| `dim_trips` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `dim_trips` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time in `PROCESSOR_TIMEZONE`. |
| `dim_trips` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `dim_trips` | `act_start_time` | first normalized stop + realtime data | Uses `act_departure_time` from the first entry in `normalized_stop_times` (sorted by `stop_sequence`) when present; otherwise uses that stop's `nom_departure_time`. The first normalized stop is the stop with the lowest `stop_sequence` that carries any realtime coverage; it may differ from the nominal first stop when the nominal first stop has no realtime data and no preceding update to propagate from. `schedule_relationship` of the source stop is irrelevant. |
| `dim_trips` | `act_end_time` | last normalized stop + realtime data | Uses `act_arrival_time` from the last entry in `normalized_stop_times` (sorted by `stop_sequence`) when present; falls back to `act_departure_time` of that stop; falls back to `nom_departure_time`. Written on every update regardless of `schedule_relationship` and regardless of whether the timestamp is in the past or future. The last normalized stop may be a propagated (SCHEDULED) stop when the nominal last stop is not explicitly covered by the feed. |
| `dim_trips` | `act_total_distance` | `nom_total_distance` (SCHEDULED) or `0.0` (all others) | Derived by `LoadingService._derive_realtime_trip_fields` from the trip's `schedule_relationship`. If `schedule_relationship == SCHEDULED`, `act_total_distance` is set to the trip's `nom_total_distance` (sourced from the nominal stop-time baseline). For all other relationships (`UNKNOWN`, `CANCELLED`, …) `act_total_distance` is set to `0.0`. **Limitation:** this is an all-or-nothing attribution model. A trip that was only partially operated is treated identically to a fully operated trip (SCHEDULED). Per-segment or stop-level partial-distance accounting is not supported in this version. |
| `dim_trips` | `schedule_relationship` | `TripDescriptor.schedule_relationship` | Latest-wins update from trip-level schedule relationship. |
| `dim_trips` | `operator_id` | n/a | Always `null`. |
| `dim_trips` | `operator_name` | n/a | Always `null`. |
| `dim_trips` | `concessionaire_id` | n/a | Not set by this pipeline. Owned exclusively by the nominal pipeline. The realtime path issues a pure `UPDATE` targeting only `act_*` fields and `schedule_relationship`, so this column is never touched by realtime writes. |
| `dim_trips` | `concessionaire_name` | n/a | Not set by this pipeline. Owned exclusively by the nominal pipeline. The realtime path issues a pure `UPDATE` targeting only `act_*` fields and `schedule_relationship`, so this column is never touched by realtime writes. |
| `fact_stop_times` | `instance_id` | runtime instance | Inject from scheduler pipeline context. |
| `fact_stop_times` | `operation_day_date` | `TripDescriptor.start_date` or runtime fallback | Use `TripDescriptor.start_date` when present; otherwise use current calendar date at processing time in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `trip_id` | `TripUpdate.trip.trip_id` | Direct mapping after trip resolution. |
| `fact_stop_times` | `stop_id` | `StopTimeUpdate.stop_id` or mapped from `stop_sequence` | Prefer explicit stop_id; otherwise resolve via nominal stop sequence mapping. |
| `fact_stop_times` | `stop_sequence` | `StopTimeUpdate.stop_sequence` | Direct mapping when present; required for deterministic ordering, nominal matching, and as primary key column. |
| `fact_stop_times` | `distance_from_start` | nominal baseline | Taken from the matching nominal stop-time row (keyed by `stop_sequence`) in the central load service. Set to `0.0` as an initial placeholder; replaced with the correct nominal value before every DB write when nominal data is loaded. Never computed from `stop_sequence` or event timestamps. |
| `fact_stop_times` | `act_arrival_time` | `StopTimeEvent.time` or `StopTimeEvent.delay` | Prefer absolute `time`; if absent, use `delay` as offset to nominal arrival time; persist in `PROCESSOR_TIMEZONE`. |
| `fact_stop_times` | `act_departure_time` | `StopTimeEvent.time` or `StopTimeEvent.delay` | Prefer absolute `time`; if absent, use `delay` as offset to nominal departure time; persist in `PROCESSOR_TIMEZONE`. |

For one stop-time row, if exactly one of `act_arrival_time` / `act_departure_time` can be resolved, the missing counterpart is synchronized to the available value before boundary calculations.
| `fact_stop_times` | `schedule_relationship` | `StopTimeUpdate.schedule_relationship` | Latest-wins update per stop row; default from trip-level if missing and policy applies. |

### StopTimeEvent Timestamp Priority

For each `StopTimeEvent` (`arrival`, `departure`), the resolver uses this strict order:

1. `event.time` (absolute UNIX timestamp) is authoritative and always preferred.
2. `event.delay` is used only when `event.time` is absent.
3. Delay fallback is computed as offset to the matching nominal stop-time value (arrival/departure respectively).

All resolved timestamps from steps above are converted to `PROCESSOR_TIMEZONE` before persistence.

### Trip Boundary Time Derivation

- `act_start_time` is always derived from the first stop-time row (stop order), not by taking a global minimum over all stops.
- `act_end_time` is always derived from the last stop-time row (stop order), not by taking a global maximum over all stops.
- Trip boundary ordering uses `stop_sequence` (with nominal departure tie-breaker) to identify first/last rows.
- If first/last stop realtime departure values are unavailable, fallback uses nominal departure times from those same boundary rows.
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

3. Remaining mutable rows are upserted:
- Everything that is not frozen by the rules above is upserted.

## Implementation Details

The GTFSRT-TRIPUPDATES pipeline should be implemented in streaming fashion end-to-end, with bounded in-memory state and idempotent upsert behavior.

> **Limitation — actual distance is all-or-nothing:**
> `act_total_distance` is derived at trip level using a binary rule: `SCHEDULED` trips receive the full `nom_total_distance` as their actual distance; all other trips (`UNKNOWN`, `CANCELLED`, …) receive `0.0`. This model does not distinguish trips that were operated in full from trips that were only partially operated (e.g. curtailed at an intermediate stop). A trip operated from stop 1 to stop 4 out of 10 nominal stops is attributed the same `act_total_distance` (`0.0`) as a fully cancelled trip, unless its `schedule_relationship` is explicitly `SCHEDULED`. Partial-distance accounting per stop segment is not supported in this version and requires a future design extension.

### LoadingService — nominal matching and realtime field derivation

`LoadingService.load_realtime_trip_and_stop_times` is the single owner of all nominal-matching and realtime field derivation logic. The pipeline hands it an unmatched `TripRecord` (with `act_total_distance=None`); the service returns nothing — it performs all writes itself. The internal execution order is:

1. **Primary trip-id lookup** — `get_nominal_stop_times_for_trip(trip.trip_id)` looks up stop times using the `trip_id` supplied by the feed.
2. **Alternative matching fallback** — when the primary lookup returns no rows (the feed's `trip_id` does not exist in `dim_trips`), `_resolve_nominal_trip_id` attempts a secondary lookup by `route_id` + `start_time` string against the nominal schedule. If this also finds no match, the update is discarded.
3. **trip_id remapping** — when the fallback match succeeds, both `trip.trip_id` and all `stop_time.trip_id` values are rewritten to the resolved nominal `trip_id` before any further processing. All subsequent steps use this remapped identifier.
4. **Nominal baseline merge** — `_apply_nominal_baseline` aligns realtime stop updates against the nominal stop sequence by `stop_sequence` and propagates the last known delay forward to stops that have no explicit update.
5. **Stop-time normalization** — `_normalize_realtime_stop_times` resolves each stop's `act_arrival_time` and `act_departure_time` from absolute timestamps or delay offsets; mirrors the available side when only one exists.
6. **Nominal trip fetch** — `get_nominal_trip(trip.trip_id)` reads the full stored `TripRecord` from `dim_trips` using the **already-remapped** `trip_id`. This is intentional: a trip matched via the alternative fallback in step 2–3 will be fetched here by its nominal `trip_id`, not by the feed's original identifier. The stored `TripRecord` carries the authoritative `nom_total_distance` as written by the nominal pipeline (including any value derived from the shape index).
7. **Realtime trip field derivation** — `_derive_realtime_trip_fields` computes `act_start_time`, `act_end_time`, `nom_total_distance`, and `act_total_distance`:
   - `nom_total_distance`: the value from the nominal `TripRecord` fetched in step 6 is preferred; `max(distance_from_start)` over nominal stop times is used as fallback only when no stored nominal trip was found.
   - `act_total_distance`: `SCHEDULED` → equal to `nom_total_distance`; all other relationships (`UNKNOWN`, `CANCELLED`, …) → `0.0`.
   - `act_start_time` / `act_end_time`: derived from the first and last entries in the normalized stop-time list respectively.
8. **Persistence** — `upsert_realtime_trip` and `upsert_realtime_stop_times` write the derived values using a pure `UPDATE` on `dim_trips` (no insert, so phantom rows are impossible) and an upsert on `fact_stop_times`.

### End-to-end flow

1. Open endpoint stream and decode protobuf feed message in streaming mode.
2. Iterate TripUpdate entities incrementally.
3. Resolve trip identity and skip closed/completed trips.
4. Iterate stop_time_update rows and resolve realtime timestamps for all rows.
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
- Closed trips are excluded from transformation.

### Error handling and observability

- Reject unresolved or invalid trip descriptors with structured diagnostics.
- Report counters for: entities received, entities filtered, completed-trip skips, and successful upserts.
- Log late/out-of-order updates when they lose latest-wins arbitration.
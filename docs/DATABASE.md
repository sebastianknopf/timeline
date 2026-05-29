# Database Architecture

This document defines the database schema and model for Timeline.

For the overall system architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).

## Modeling Principle

The database uses one central fact table and three dimensions:

- fact table: `fact_stop_times`
- dimensions: `dim_stops`, `dim_routes`, `dim_trips`

`instance_id` is mandatory in all tables and is always part of relational keys. This enforces tenant-like isolation so data from different instances remains separated.

## Type Rules

These type rules are applied consistently:

- date fields: `date`
- time fields: `timestamptz`
- distance fields: `double precision`
- sequence fields: `integer`
- all remaining fields: `text`

Stop coordinates are also stored as `double precision`:

- `stop_lat`
- `stop_lon`

Applied date and time fields:

- date: `operation_day_date`
- timestamps:
  - `nom_arrival_time`
  - `nom_departure_time`
  - `act_arrival_time`
  - `act_departure_time`
  - `nom_start_time`
  - `nom_end_time`
  - `act_start_time`
  - `act_end_time`

Distance and coordinate fields:

- `distance_from_start`
- `nom_total_distance`
- `act_total_distance`
- `stop_lat`
- `stop_lon`

## Table Definitions

### dim_stops

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `stop_id` | `text` | no | Stop identifier within instance |
| `stop_name` | `text` | no | Human-readable stop name |
| `stop_lat` | `double precision` | no | Latitude |
| `stop_lon` | `double precision` | no | Longitude |

Primary key:

- (`instance_id`, `stop_id`)

### dim_routes

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `route_id` | `text` | no | Route identifier within instance |
| `route_name` | `text` | no | Human-readable route name |
| `concessionaire_id` | `text` | yes | Concessionaire identifier |
| `concessionaire_name` | `text` | yes | Concessionaire name |
| `operator_id` | `text` | yes | Operator identifier |
| `operator_name` | `text` | yes | Operator name |

Primary key:

- (`instance_id`, `route_id`)

### dim_trips

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `operation_day_date` | `date` | no | Operation day |
| `trip_id` | `text` | no | Trip identifier within instance/day |
| `route_id` | `text` | no | Route identifier; references `dim_routes` |
| `concessionaire_id` | `text` | yes | Concessionaire identifier |
| `concessionaire_name` | `text` | yes | Concessionaire name |
| `operator_id` | `text` | yes | Operator identifier |
| `operator_name` | `text` | yes | Operator name |
| `nom_start_time` | `timestamptz` | no | Planned start timestamp |
| `nom_end_time` | `timestamptz` | no | Planned end timestamp |
| `act_start_time` | `timestamptz` | yes | Actual start timestamp |
| `act_end_time` | `timestamptz` | yes | Actual end timestamp |
| `nom_start_stop_id` | `text` | no | Planned trip start stop |
| `nom_end_stop_id` | `text` | no | Planned trip end stop |
| `nom_total_distance` | `double precision` | no | Planned total trip distance |
| `act_total_distance` | `double precision` | yes | Actual total trip distance |
| `schedule_relationship` | `text` | no | Default: `UNKNOWN` |

Primary key:

- (`instance_id`, `operation_day_date`, `trip_id`)

Foreign keys:

- (`instance_id`, `route_id`) references `dim_routes` (`instance_id`, `route_id`) — ON DELETE RESTRICT
- (`instance_id`, `nom_start_stop_id`) references `dim_stops` (`instance_id`, `stop_id`) — ON DELETE RESTRICT
- (`instance_id`, `nom_end_stop_id`) references `dim_stops` (`instance_id`, `stop_id`) — ON DELETE RESTRICT

### fact_stop_times

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `operation_day_date` | `date` | no | Operation day |
| `trip_id` | `text` | no | Trip identifier |
| `stop_id` | `text` | no | Stop identifier |
| `stop_sequence` | `integer` | no | Stop order within one trip/day (from GTFS `stop_sequence`) |
| `distance_from_start` | `double precision` | no | Distance from trip origin |
| `nom_arrival_time` | `timestamptz` | no | Planned arrival timestamp |
| `nom_departure_time` | `timestamptz` | no | Planned departure timestamp |
| `act_arrival_time` | `timestamptz` | yes | Actual arrival timestamp |
| `act_departure_time` | `timestamptz` | yes | Actual departure timestamp |
| `schedule_relationship` | `text` | no | Default: `UNKNOWN` |

Primary key:

- (`instance_id`, `operation_day_date`, `trip_id`, `stop_id`, `stop_sequence`)

Foreign keys:

- (`instance_id`, `stop_id`) references `dim_stops` (`instance_id`, `stop_id`) — ON DELETE RESTRICT
- (`instance_id`, `operation_day_date`, `trip_id`) references `dim_trips` (`instance_id`, `operation_day_date`, `trip_id`) — ON DELETE RESTRICT

Design note:

- `stop_sequence` is the authoritative identifier for a stop position within one trip/day. Using `stop_sequence` in the primary key — rather than `distance_from_start` — ensures that upserts correctly update an existing row when distances change (for example when a new GTFS static feed provides or corrects `shape_dist_traveled` values). `distance_from_start` remains a regular column so its value can be updated on every pipeline run.

## Model Relationships

Cardinality:

- one `dim_stops` row can be referenced by many `fact_stop_times` rows
- one `dim_trips` row can be referenced by many `fact_stop_times` rows
- one `dim_stops` row can also be referenced by many `dim_trips` rows through `nom_start_stop_id` and `nom_end_stop_id`
- one `dim_routes` row can be referenced by many `dim_trips` rows through `route_id`

All foreign key relationships enforce referential integrity with `ON DELETE RESTRICT`. Deleting a parent row that is still referenced by a child row will fail immediately.

Logical model flow:

1. `dim_stops` contains normalized stop metadata per instance.
2. `dim_routes` contains normalized route metadata per instance, including concessionaire and operator information.
3. `dim_trips` contains trip-level aggregates per instance and operation day, linked to a route in `dim_routes`.
4. `fact_stop_times` contains stop-time facts linked to both `dim_stops` and `dim_trips`.

## Index Strategy

The following indexes should be created to keep joins and instance-scoped filtering efficient.

### Required indexes from primary keys

- `dim_stops`: pk (`instance_id`, `stop_id`)
- `dim_routes`: pk (`instance_id`, `route_id`)
- `dim_trips`: pk (`instance_id`, `operation_day_date`, `trip_id`)
- `fact_stop_times`: pk (`instance_id`, `operation_day_date`, `trip_id`, `stop_id`, `stop_sequence`)

### Additional recommended indexes

`dim_stops`:

- idx for stop-name lookups per instance: (`instance_id`, `stop_name`)

`dim_routes`:

- idx for concessionaire-based filtering per instance: (`instance_id`, `concessionaire_id`)
- idx for operator-based filtering per instance: (`instance_id`, `operator_id`)

`dim_trips`:

- idx for route-based filtering per instance/day: (`instance_id`, `operation_day_date`, `route_id`)
- idx for operator-based filtering per instance/day: (`instance_id`, `operation_day_date`, `operator_id`)
- idx for concessionaire-based filtering per instance/day: (`instance_id`, `operation_day_date`, `concessionaire_id`)

`fact_stop_times`:

- idx for stop-centric lookups per instance/day: (`instance_id`, `operation_day_date`, `stop_id`)
- idx for trip timelines per instance/day: (`instance_id`, `operation_day_date`, `trip_id`)
- idx for ordered trip stop-time resolution per instance/day: (`instance_id`, `operation_day_date`, `trip_id`, `stop_sequence`)
- idx for actual-time range queries: (`instance_id`, `act_arrival_time`)
- idx for actual-departure range queries: (`instance_id`, `act_departure_time`)

Indexing principle:

- Keep `instance_id` as the leading index column whenever possible to guarantee tenant-pruned execution paths.

## Model Modules in Processor Code

The processor intentionally uses two different `models.py` modules with different responsibilities.

Database ORM models (`processor/src/processor/database/models.py`):

- SQLAlchemy declarative classes for persisted tables (`dim_stops`, `dim_routes`, `dim_trips`, `fact_stop_times`)
- include table metadata, primary keys, foreign keys, indexes, and database column types
- used by Alembic metadata wiring and concrete repository persistence logic

Loading models (`processor/src/processor/loading/models.py`):

- dataclass records used at service boundaries between pipeline, mapping, and loading layers
- represent normalized runtime payloads independent from SQLAlchemy session state
- keep orchestration and matching logic testable without direct ORM dependencies

These two modules are not duplicates.
They represent different architectural layers (persistence schema vs. runtime transfer models) and are both required.
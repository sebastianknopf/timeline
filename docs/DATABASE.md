# Database Architecture

This document defines the database schema and model for Timeline.

For the overall system architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).
For database access scripts, see [docs/SCRIPTS.md](SCRIPTS.md).

## Modeling Principle

The database uses one central fact table and three dimensions:

- fact table: `fact_stop_times`, `fact_requests`, `fact_quality_issues`
- dimensions: `dim_stops`, `dim_routes`, `dim_trips`, `dim_issue_types`

`instance_id` is mandatory in most tables and is always part of relational keys. This enforces tenant-like isolation so data from different instances remains separated. Only system dimension tables like `dim_issue_types` are build with a single primary key without `instance_id`.

## Type Rules

These type rules are applied consistently:

- date fields: `date`
- time fields: `timestamptz`
- distance fields and coordinates: `double precision`
- sequence fields: `integer`
- counting fields: `integer`
- all remaining fields: `text`

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
| `realtime_assignment_method` | `text` | yes | Indicates the internal realtime assignment method. Available values are `DIRECT` and `MATCHING`. Default: `NULL` |

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

### dim_issue_types

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `id` | `integer` | no | Inssue type ID |
| `code` | `text` | no | Coded name for the quality issue |

Primary key:

- (`id`)

Important note: This table is a system-only table and must not be changed by any pipeline! The intention behind the table is to keep the indexes smaller and normalize the data quality issue model. This table is only filled by corresponding database migrations. 

See more about the available issue codes in [docs/DATAQUALITY.md](docs/DATAQUALITY.md).

### fact_requests

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `request_id` | `text` | no | Hash over `pipeline_id` and `timestamp` to minimize the size of indexes |
| `pipeline_id ` | `text` | no | The ID of the pipeline which triggered this pipeline exection |
| `timestamp ` | `timestamptz` | no | Timestamp when the pipeline exection was triggered |
| `num_entities` | `integer` | no | Number of generally processable entities in the payload |
| `age_seconds` | `integer` | no | Age of the payload in seconds. Extraction depends on the pipeline implementation |
| `status_code ` | `integer` | no | HTTP-like status code for the pipeline execution. Default: 200 |

Primary key:

- (`instance_id`, `request_id`)

### fact_quality_issues

| Column | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `instance_id` | `text` | no | Instance scope key |
| `issue_id` | `text` | no | Hash over `pipeline_id`, `timestamp`, `entity_id` and `issue_type_id`  to minimize the size of indexes |
| `pipeline_id ` | `text` | no | The ID of the pipeline which triggered this pipeline exection |
| `timestamp ` | `timestamptz` | no | Timestamp when the pipeline exection was triggered |
| `entity_id ` | `text` | no | Exactly NOT `trip_id` because `trip_id` describes a particular trip in nominal data. Here, we're not matched yet and so we only can talk about the `entity_id` |
| `issue_type_id ` | `integer` | no | Reference to the issue type |
| `concessionaire_id` | `text` | yes | Concessionaire identifier |
| `concessionaire_name` | `text` | yes | Concessionaire name |
| `operator_id` | `text` | yes | Operator identifier |
| `operator_name` | `text` | yes | Operator name |
| `assessment_value` | `text` | yes | The detected value which leaded to the quality issue |
| `num_affected_values` | `integer` | no | Number of affected values in one entity. Default: 1 |

Primary key:

- (`instance_id`, `issue_id`)

Foreign keys:

- (`issue_type_id`) references `dim_issue_types` (`id`) — ON DELETE CASCADE

## Index Strategy

The following indexes should be created to keep joins and instance-scoped filtering efficient.

### Required indexes from primary keys

- `dim_stops`: pk (`instance_id`, `stop_id`)
- `dim_routes`: pk (`instance_id`, `route_id`)
- `dim_trips`: pk (`instance_id`, `operation_day_date`, `trip_id`)
- `fact_stop_times`: pk (`instance_id`, `operation_day_date`, `trip_id`, `stop_id`, `stop_sequence`)
- `dim_issue_types`: pk (`id`)
- `fact_requests`: pk (`instance_id`, `request_id`)
- `fact_quality_issues`: (`instance_id`, `issue_id`)

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

`fact_requests`:

- idx for showing data input and status per instance/pipeline/timerange: (`instance_id`, `pipeline_id`, `timestamp`)

`fact_quality_issues`:

- idx for pipeline-/issue-centric lookup per timestamp: (`instance_id`, `pipeline_id`, `timestamp`, `issue_type_id`)
- idx for operator-/concessionaire-centric lookup per timestamp: (`instance_id`, `pipeline_id`, `concessionaire_id`, `operator_id`)

Indexing principle:

- Keep `instance_id` as the leading index column whenever possible to guarantee tenant-pruned execution paths.

## Model Modules in Processor Code

The processor intentionally uses two different `models.py` modules with different responsibilities.

Database ORM models (`processor/src/processor/database/models.py`):

- SQLAlchemy declarative classes for persisted tables (`dim_stops`, `dim_routes`, `dim_trips`, `fact_stop_times`, `dim_issue_types`, `fact_requests`, `fact_quality_issues`)
- include table metadata, primary keys, foreign keys, indexes, and database column types
- used by Alembic metadata wiring and concrete repository persistence logic

Loading models (`processor/src/processor/loading/models.py`):

- dataclass records used at service boundaries between pipeline, mapping, and loading layers
- represent normalized runtime payloads independent from SQLAlchemy session state
- keep orchestration and matching logic testable without direct ORM dependencies

Export models (`processor/src/processor/exports/models.py`):

- dataclass records used at service boundaries for the export interfaces
- represent normalized runtime payloads independent from SQLAlchemy session state
- keep orchestration and matching logic testable without direct ORM dependencies

These modules are not duplicates.
They represent different architectural layers (persistence schema vs. runtime transfer models) and are both required.
# Processor Architecture

This document describes the architecture of the `processor` service.

For the overall repository architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).

## Purpose

The processor is the Python service responsible for the Timeline ETL workload. It is structured as a containerized application and runs as a module entrypoint through `python -m processor`.

Its runtime logic is based on two coordinated data pipelines per instance:

- nominal pipeline for planned schedule data
- realtime pipeline for observed operational data

## Packaging Layout

The processor project uses a `src` layout and is managed as a standalone Python package.

- Python project root: `processor/`
- Application package: `processor/src/processor/`
- Runtime entrypoint: `processor.__main__:main`
- Build metadata: `processor/pyproject.toml`

## Runtime Characteristics

The service runs inside Docker and receives its database connection through the `PROCESSOR_DATABASE_URL` environment variable inside the container.

The scheduler timezone is configured through `PROCESSOR_TIMEZONE` (IANA timezone string like `Europe/Berlin`).
If not set, `UTC` is used.

The scheduler configuration is read from a YAML file mounted in the container at `/app/config/config.yaml`.

Before the scheduler starts, the processor executes Alembic migrations to `head` during startup.
If migration execution fails, service startup fails fast and no pipeline scheduling begins.

The container is intentionally minimal:

- no host port is published
- access happens through the internal Compose network
- startup depends on PostgreSQL health readiness

## Processing Model

The processor maintains pipeline execution per instance (tenant-like scope).

An instance represents an isolated operational domain. Data produced for one instance must remain separable from all other instances in:

- persistence (database records)
- analytics and visualizations (dashboards)

### Pipeline Processing Scope

Pipelines are responsible for extraction and transformation only.

Pipeline runs may execute asynchronously and concurrently across multiple instances.
Pipeline implementations must therefore be safe to execute in parallel without shared mutable state across instances.

For each run, a pipeline:

1. fetches source data from the configured endpoint
2. normalizes the extracted payload into the internal canonical format
3. hands normalized records to the central load service

Loading into PostgreSQL is not performed directly by pipeline implementations.
Pipelines must not query or write the database directly.

### Central Mapping Service

The central mapping service is invoked by the pipeline before transformation and matching steps that require identifier translation.

The service must be independent per instance because pipelines may run asynchronously and multiple instances may run at the same time.
No cross-instance mutable mapping state may be shared at runtime.

Its responsibilities are:

- read mapping file paths from pipeline configuration
- load each mapping source into in-memory dictionaries of `key -> value`
- provide normalized mapping lookup APIs to the pipeline runtime
- provide mapping-to-loading conversion methods so pipelines can pass mapped `TripRecord` and `StopTimeRecord` objects to the loading service
- apply wildcard mapping keys (for example `*`) according to mapping resolution rules

Mapping ownership decision:

- mapping file parsing and mapping application are owned by the central mapping service
- the central load service receives already mapped identifiers and focuses on persistence and matching

### Central Load Service

The central load service owns all load-phase responsibilities:

- receiving normalized data from pipelines and loading it into the database
- being the only service allowed to interact with PostgreSQL directly for both reads (queries/matching lookups) and writes (insert/upsert/update)
- applying upsert-based persistence as the default write strategy
- handling matching workflows when realtime records arrive without corresponding nominal records
- resolving trip identity after mapped identifiers are provided by the central mapping service
- ensuring atomic database behavior even when asynchronous pipeline runs call the service concurrently
- resolving realtime `StopTimeEvent` values by preferring absolute timestamps and using delay-as-offset-to-nominal fallback
- enforcing realtime update scope so conflict updates only touch `act_*` and `schedule_relationship`
- deriving `dim_trips.act_start_time` and `dim_trips.act_end_time` from first/last nominal departure rows ordered by `stop_sequence` (with nominal departure tie-break), with nominal departure fallback when realtime values are missing
- persisting `dim_trips.act_end_time` only after the derived end candidate timestamp is reached (`<= now`), otherwise keeping it `NULL`


Atomicity and asynchronous concurrency requirements:

- execute each load request in an explicit database transaction
- define the transaction unit as one instance and one pipeline-run payload
- include upsert, matching, and related writes in the same transaction boundary
- commit only after all operations succeed, otherwise roll back the entire transaction
- use deterministic concurrency control during matching and upsert, for example row-level locks (`SELECT ... FOR UPDATE`) or PostgreSQL advisory locks scoped by instance and trip identity
- treat deadlocks and serialization conflicts as retriable errors with bounded retries


Matching workflow for realtime-to-nominal trip resolution:

1. Try direct trip ID match first.
2. If a direct trip ID match exists, accept it and stop matching.
3. If no direct trip ID match exists, run fallback matching:
   - use mapped actual route ID from the central mapping service
   - use mapped actual stop IDs from the central mapping service
   - find a nominal trip where:
     - mapped route ID equals nominal route ID
     - actual trip start time equals nominal trip start time
     - mapped stop IDs equal nominal stop IDs in the same sequence
4. If a fallback candidate satisfies all conditions above, treat that nominal trip as the final matched trip.
5. Final fallback when start time and start date are unavailable in realtime input:
  - match by mapped stop IDs in the same sequence
  - validate that actual departure times match nominal departure times within a tolerance window of `-10 minutes` to `+30 minutes`
  - if multiple nominal trips satisfy the fallback criteria, select the best match with the smallest arrival/departure time deviation
  - if all stops satisfy sequence and tolerance conditions, treat that nominal trip as the final matched trip

### Repository Service Layer

Database I/O for the central load service is encapsulated by a dedicated repository service abstraction.

Responsibilities of the repository layer:

- expose explicit interface methods for nominal inserts (`dim_stops`, `dim_trips`, `fact_stop_times`)
- expose explicit interface methods for realtime upserts on trips and stop times
- keep SQLAlchemy and SQL details out of the central load service business logic

Central load service dependency rule:

- the central load service depends only on repository interfaces (abstract contracts), not concrete persistence classes
- concrete repository implementations are injected at composition/startup time

This separation keeps matching logic and orchestration testable without a live database.

### Nominal Pipeline

The nominal pipeline ingests and maintains planning data, such as schedule definitions and expected operations.

Its responsibility is to keep the planned state up to date as the baseline for later comparison and analysis.

Nominal pipelines typically run at low frequency, commonly once per day.

### Realtime Pipeline

The realtime pipeline ingests operational data captured during the day of operation.

Its responsibility is to reflect what actually happened and provide the observed state for monitoring and downstream analysis.

Realtime pipelines typically run at high frequency, commonly at least once per minute.

### Scheduler Ownership and Lifecycle

Each pipeline run is triggered by a scheduler.

The scheduler is started by the processor entrypoint during service startup and keeps both pipelines active over time for every configured instance.

Before scheduling begins, the scheduler reads and validates the configuration file structure and required keys.

Only valid instances and pipelines are activated. Invalid configuration must fail startup instead of running partially configured pipelines.

Scheduling is configured per pipeline through the pipeline `cron` expression.

Cron schedules are evaluated in the configured processor timezone (`PROCESSOR_TIMEZONE`).
This timezone also defines the scheduler's "current time" reference for calculating delays until the next run.

The scheduler supports both minute-based five-field cron expressions and second-based six-field cron expressions.
Example second-level schedules include `*/10 * * * * *` (every 10 seconds).

Each scheduled pipeline run is executed asynchronously so that high-frequency realtime jobs do not block other pipelines.

Scheduler overlap behavior:

- a pipeline is uniquely identified by the pair `instance.id` + `pipeline.id`
- at most one run of the same pipeline pair may execute concurrently
- if the next cron tick occurs while that pipeline pair is still running, that tick is skipped (no queued backlog for skipped ticks)
- pipelines from different instances may still run concurrently

Expected operation pattern:

- nominal pipelines should run once per day
- realtime pipelines should run at least once per minute

For high-frequency use cases, realtime pipelines can also be scheduled on second granularity.

At a high level:

1. Service entrypoint boots the processor runtime.
2. Scheduler is initialized.
3. Scheduler registers schedules from each pipeline `cron` expression.
4. Scheduler executes pipeline runs asynchronously per schedule.
5. Pipelines execute per instance with isolated data boundaries and may run concurrently across instances.

## Dependencies

The processor currently depends on:

- PostgreSQL for persistent storage
- Alembic for schema migration support
- SQLAlchemy for database access layers
- psycopg for the PostgreSQL driver

## Execution Model

The container starts the Python module directly:

1. Docker Compose builds the processor image from `processor/Dockerfile`.
2. The container starts with `python -m processor`.
3. The entrypoint loads and validates `/app/config/config.yaml`.
4. The entrypoint runs Alembic migrations to schema `head`.
5. The entrypoint initializes the scheduler.
6. The scheduler continuously triggers nominal and realtime pipeline runs per instance using each pipeline cron expression.
7. Each scheduled run executes asynchronously, with overlap skipping for already-running instance/pipeline pairs.
8. Pipelines extract and transform data, then hand normalized output to the central load service.
9. The central load service persists isolated instance data to PostgreSQL and performs matching workflows with atomic transaction boundaries.

## Relationship to the Repository

The processor is the only application service in the repository and is the core runtime unit that the surrounding infrastructure supports.
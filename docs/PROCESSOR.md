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

The service runs inside Docker and receives its database connection through the `DATABASE_URL` environment variable.

The scheduler configuration is read from a YAML file mounted in the container at `/app/config/config.yaml`.

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

For each run, a pipeline:

1. fetches source data from the configured endpoint
2. normalizes the extracted payload into the internal canonical format
3. hands normalized records to the central load service

Loading into PostgreSQL is not performed directly by pipeline implementations.

### Central Load Service

The central load service owns all load-phase responsibilities:

- receiving normalized data from pipelines and loading it into the database
- applying upsert-based persistence as the default write strategy
- handling matching workflows when realtime records arrive without corresponding nominal records
- exposing mapping interfaces for external identifiers used by pipelines

The mapping interfaces are used by the calling pipeline to resolve and maintain:

- stop ID mappings
- route ID mappings

Matching workflow for realtime-to-nominal trip resolution:

1. Try direct trip ID match first.
2. If a direct trip ID match exists, accept it and stop matching.
3. If no direct trip ID match exists, run fallback matching:
   - map the actual route ID using route ID mappings
   - map all actual stop IDs using stop ID mappings
   - find a nominal trip where:
     - mapped route ID equals nominal route ID
     - actual trip start time equals nominal trip start time
     - mapped stop IDs equal nominal stop IDs in the same sequence
4. If a fallback candidate satisfies all conditions above, treat that nominal trip as the final matched trip.

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

Each scheduled pipeline run is executed asynchronously so that high-frequency realtime jobs do not block other pipelines.

Expected operation pattern:

- nominal pipelines should run once per day
- realtime pipelines should run at least once per minute

At a high level:

1. Service entrypoint boots the processor runtime.
2. Scheduler is initialized.
3. Scheduler registers schedules from each pipeline `cron` expression.
4. Scheduler executes pipeline runs asynchronously per schedule.
5. Pipelines execute per instance with isolated data boundaries.

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
4. The entrypoint initializes the scheduler.
5. The scheduler continuously triggers nominal and realtime pipeline runs per instance using each pipeline cron expression.
6. Each scheduled run executes asynchronously.
7. Pipelines extract and transform data, then hand normalized output to the central load service.
8. The central load service persists isolated instance data to PostgreSQL and performs matching and identifier mapping workflows.

## Relationship to the Repository

The processor is the only application service in the repository and is the core runtime unit that the surrounding infrastructure supports.
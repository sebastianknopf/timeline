Apply
# Configuration Architecture

This document defines the runtime configuration model consumed by the processor.

For the overall system architecture, see [docs/ARCHITECTURE.md](ARCHITECTURE.md).

## File Format and Location

- Format: YAML only
- Host path: `config/config.yaml`
- Container path: `/app/config/config.yaml`

The repository provides a template at `config/config.yaml.example`.

## Top-Level Structure

The top-level key is `instance` and must contain at least one instance object.

Each instance defines:

- `id`: unique instance identifier
- `pipeline`: list of pipeline objects (one or more)
- `export` (optional): list of export objects

For export configuration details, see [docs/EXPORTS.md](EXPORTS.md).

## Pipeline Object Structure

Each pipeline object defines:

- `id`: pipeline identifier used in logs
- `name`: pipeline definition name used to select implementation behavior (for example `gtfs` or `gtfsrt-tripupdates`)
- `type`: pipeline type, expected values are `nominal` or `realtime`
- `cron`: cron expression used to schedule this pipeline
- `endpoint`: source endpoint URL or address
- `policy` (optional): execution policy for cron scheduling behavior. Allowed values are `schedule` and `startupAndSchedule`. Default is `schedule`.
- `authentication` (optional): authentication object when required by the endpoint
- `parameters` (optional): object containing arbitrary pipeline-specific key/value parameters passed to the selected pipeline implementation
- `filter`: filter object containing either `routes` or `operators`
  - `routes`: list of route filters
    - `match`: wildcard pattern for route filtering
    - `type`: filter type, either `include` or `exclude`
    - `mapping`: mapping file configuration for identifier translation inputs
      - `stops`: path to a stops mapping file
      - `routes`: path to a routes mapping file

Pipeline policy behavior:

- `schedule`: execute only on cron ticks.
- `startupAndSchedule`: execute once at processor startup and then continue executing on cron ticks.

`mapping` can contain these optional keys:

- `stops`: path to a stops mapping file
- `routes`: path to a routes mapping file

Mapping files must provide at least these columns:

- `key`
- `value`

Additional descriptive columns are allowed, but they are ignored by the service.

Mapping files may include a header row. If a header row is present, it is not treated as mapping data.

Wildcard behavior:

- An asterisk (`*`) in a mapping `key` is treated as a wildcard pattern during mapping resolution.

Path resolution for `mapping.stops` and `mapping.routes`:

- relative paths are resolved against the processor mapping root in the container (`/etc/mapping`)
- absolute paths must be under `/etc/mapping`

Pipeline definition documents are located in [docs/pipelines/GTFS.md](pipelines/GTFS.md) and [docs/pipelines/GTFSRT-TRIPUPDATES.md](pipelines/GTFSRT-TRIPUPDATES.md).

Detailed behavioral semantics of `nominal` and `realtime` pipelines are documented in [docs/PROCESSOR.md](PROCESSOR.md).

Cron expression support:

- five-field format (minute precision), for example `* * * * *`
- six-field format (second precision), for example `*/10 * * * * *`

Authentication supports exactly one of these shapes:

- token authentication: `token`
- basic credentials: `username` and `password`

If the endpoint does not require authentication, the `authentication` key can be omitted.

## Configuration Validation Rules

At startup, the processor must read and validate the YAML configuration before runtime execution begins.

Minimum validation requirements:

1. YAML is syntactically valid.
2. Top-level `instance` key exists and contains at least one object.
3. Each instance has non-empty `id` and at least one `pipeline`.
4. Each pipeline has non-empty `id`, `name`, `type`, `cron`, and `endpoint`.
5. Pipeline `type` is either `nominal` or `realtime`.
6. Pipeline `name` matches a known pipeline definition document.
7. If `policy` is present, it must be either `schedule` or `startupAndSchedule`.
8. If `authentication` is present, it is either:
   - `{ token: <value> }`
   - `{ username: <value>, password: <value> }`
9. If `parameters` is present, it must be a YAML object (mapping). Its keys are pipeline-defined and may vary by pipeline name.
10. Each pipeline has a `filter` object containing either `routes` or `operators`.
11. If `filter` contains `routes`, each route filter must have non-empty `match`, `type` and may contain references to mapping files in `mapping`.
12. If `filter` contains `operators`, each operator filter must have non-empty `match`, `type` and may contain references to mapping files in and valid `mapping`.
   - If the key `match` contains a `*` this is interpreted as wildcard during the filtering.
   - The key `type` must have the value `exclude` or `include`. The value decides whether the objects matching the given value in `match` are included or expluded by the pipeline.
   - If mapping files are provided, they must be readable CSV files containing at least `key` and `value` columns and encoded as UTF-8.
   - If a mapping key contains `*`, it must be interpreted as a wildcard pattern by the mapping resolver.

If validation fails, startup must fail and no pipelines are run.

## Baseline Requirement

Each instance should configure at least:

- one `nominal` pipeline
- one `realtime` pipeline

Recommended cadence:

- `nominal`: once per day (for example `0 2 * * *`)
- `realtime`: at least once per minute (for example `* * * * *`)

Optional high-frequency cadence for realtime pipelines:

- every 10 seconds (for example `*/10 * * * * *`)

## Mapping Directory

The processor mounts a mapping root directory to `/etc/mapping` in the container.

- Host mapping root is configured by environment variable `PROCESSOR_MAPPING_DIR`
- Default value is the current project directory (`.`)

This allows mapping paths in `config.yaml` to be portable across local and container execution.
## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `PROCESSOR_DATABASE_URL` | yes | n/a | PostgreSQL connection URL used by the repository. |
| `PROCESSOR_CONFIG_PATH` | no | `/app/config/config.yaml` | Absolute path to the processor YAML configuration file. |
| `PROCESSOR_MAPPING_ROOT` | no | `/etc/mapping` | Root directory for resolving relative mapping file paths. |
| `PROCESSOR_ALEMBIC_INI_PATH` | no | `/app/alembic.ini` | Path to the Alembic INI file used for database migrations. |
| `PROCESSOR_TIMEZONE` | no | `UTC` | IANA timezone name (for example `Europe/Berlin`) used as the processor runtime timezone for scheduler and date/time handling. |
``
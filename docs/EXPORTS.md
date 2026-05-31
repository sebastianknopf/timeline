# Exports

This document defines the export configuration for the processor.

For the overall configuration model, see [docs/CONFIGURATION.md](CONFIGURATION.md).

## Overview

Exports are optional, independently scheduled jobs that extract data from the timeline and write it to files. They are configured at the same level as pipelines, inside each instance block.

Each export runs on its own cron schedule and writes output to a configurable directory inside the processor container. That directory can optionally be mapped to a host path via the `PROCESSOR_EXPORT_DIR` environment variable in Docker Compose.

## Top-Level Structure

Exports are defined under the `export` key of an instance object. The key is optional; if omitted, no exports run for that instance.

```yaml
instance:
  - id: "demo-tenant"
    pipeline:
      - ...
    export:
      - id: "daily-trip-export"
        name: "timeline-export"
        cron: "0 3 * * *"
        period:
          from: -1
          to: 0
        processing:
          directory: "/exports"
```

## Export Object Structure

Each export object defines:

- `id`: export identifier used in logs
- `name`: export definition name used to select the export implementation
- `cron`: cron expression used to schedule this export
- `period`: time window the export covers, expressed as relative day offsets from the day the export runs
- `processing` (optional): processing configuration for the export job

### `period` Object

The `period` key is mandatory and defines the range of operation days included in the export as a half-open interval `[from, to)`:

- `from`: start offset in operation days relative to the current day (inclusive)
- `to`: end offset in operation days relative to the current day (exclusive)

Both values are integers. A value of `0` refers to the current operation day on which the export runs, but since the interval is exclusive at `to`, a `to` value of `0` does **not** include the current day.

For example, `from: -1` and `to: 0` selects exactly the previous operation day.

### `processing` Object

The `processing` key currently supports one option:

- `directory` (optional): absolute path inside the processor container where export files are written. If omitted, the export implementation falls back to its own default location.

## Cron Expression Support

Export cron expressions follow the same rules as pipeline cron expressions:

- five-field format (minute precision), for example `0 3 * * *`
- six-field format (second precision), for example `*/30 * * * * *`

## Host Path Mapping

By default, export files are stored inside the processor container under the configured `processing.directory`. No host path is mapped unless `PROCESSOR_EXPORT_DIR` is explicitly set in the `.env` file.

When `PROCESSOR_EXPORT_DIR` is set, Docker Compose mounts that host directory to `/exports` inside the container:

```env
# .env
PROCESSOR_EXPORT_DIR=/path/to/host/exports
```

When `PROCESSOR_EXPORT_DIR` is not set, Docker Compose uses the named volume `processor_exports` instead, keeping the files inside the Docker-managed volume and off the host filesystem.

The `processing.directory` in the config must match the container-side mount point (`/exports`) for files to appear at the mapped host path.

## Volume Declaration

The named volume `processor_exports` is declared in `docker-compose.yml` and used automatically when `PROCESSOR_EXPORT_DIR` is not set:

```yaml
volumes:
  processor_exports:
```

## Export Definitions

### `timeline-export`

The `timeline-export` produces a ZIP archive containing flat-file extracts of the timeline database for the configured period.

#### Output Format

The export writes a single ZIP file to the configured `processing.directory`. The archive contains four comma-separated text files, one per exported table. String values that contain a comma are escaped. All files include a header row.

The file names correspond to the database table names with the `fact_` and `dim_` prefixes removed:

| File | Source table |
| --- | --- |
| `stop_times.txt` | `fact_stop_times` |
| `trips.txt` | `dim_trips` |
| `stops.txt` | `dim_stops` |
| `routes.txt` | `dim_routes` |

#### Period Filtering

All rows are filtered by `operating_day_date` against the current local date at export runtime, using the half-open interval `[from, to)` defined in the `period` configuration.

The fact table (`stop_times.txt`) drives the primary filter. The dimension tables (`trips.txt`, `stops.txt`, `routes.txt`) are filtered down to the minimum set of rows required for a valid and consistent export — only entries referenced by the filtered fact rows are included.

The `instance_id` column is omitted from all output files. Every other column from the source table is included as-is.

#### Column Reference

**`stop_times.txt`** (source: `fact_stop_times`)

| Column | Type |
| --- | --- |
| `operation_day_date` | date |
| `trip_id` | text |
| `stop_id` | text |
| `stop_sequence` | integer |
| `distance_from_start` | double |
| `nom_arrival_time` | timestamp with time zone |
| `nom_departure_time` | timestamp with time zone |
| `act_arrival_time` | timestamp with time zone (nullable) |
| `act_departure_time` | timestamp with time zone (nullable) |
| `schedule_relationship` | text |

**`trips.txt`** (source: `dim_trips`)

| Column | Type |
| --- | --- |
| `operation_day_date` | date |
| `trip_id` | text |
| `route_id` | text |
| `concessionaire_id` | text (nullable) |
| `concessionaire_name` | text (nullable) |
| `operator_id` | text (nullable) |
| `operator_name` | text (nullable) |
| `nom_start_time` | timestamp with time zone |
| `nom_end_time` | timestamp with time zone |
| `act_start_time` | timestamp with time zone (nullable) |
| `act_end_time` | timestamp with time zone (nullable) |
| `nom_start_stop_id` | text |
| `nom_end_stop_id` | text |
| `nom_total_distance` | double |
| `act_total_distance` | double (nullable) |
| `schedule_relationship` | text |

**`stops.txt`** (source: `dim_stops`)

| Column | Type |
| --- | --- |
| `stop_id` | text |
| `stop_name` | text |
| `stop_lat` | double |
| `stop_lon` | double |

**`routes.txt`** (source: `dim_routes`)

| Column | Type |
| --- | --- |
| `route_id` | text |
| `route_name` | text |
| `concessionaire_id` | text (nullable) |
| `concessionaire_name` | text (nullable) |
| `operator_id` | text (nullable) |
| `operator_name` | text (nullable) |

## Example

```yaml
instance:
  - id: "demo-tenant"
    pipeline:
      - id: "nominal-main"
        name: "gtfs"
        type: "nominal"
        cron: "0 2 * * *"
        endpoint: "https://api.example.com/schedule"
    export:
      - id: "daily-trip-export"
        name: "timeline-export"
        cron: "0 3 * * *"
        period:
          from: -1
          to: 0
        processing:
          directory: "/exports"
```

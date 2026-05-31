# timeline

Timeline is a transport operations archiving and monitoring platform. It continuously ingests planned schedule data alongside realtime operational updates and stores them together in a queryable timeline.

It is used to build a persistent, auditable record of what was planned, what actually happened, and where deviations occurred — for analysis, reporting, and operational monitoring.

In general, Timeline works as follows:

1. The processor runs per configured instance with nominal and realtime pipelines.
2. Pipelines fetch source data, normalize it, and hand it to a central load service.
3. The load layer performs matching and atomic upsert operations into PostgreSQL.
4. Dashboards consume the archived timeline data for realtime monitoring and historical analysis.

Docker and service setup for the timeline project with:

- processor (Python ETL and archiving service)
- PostgreSQL
- optional observability stack via profiles (`grafana` and `monitoring`)

## Requirements

- Docker Desktop (including Docker Compose)

## Project Structure

- docker-compose.yml: Service orchestration
- processor/: Python project in src layout with pyproject.toml, Alembic, and setuptools_scm
- grafana/: Grafana provisioning and dashboards
- monitoring/: Monitoring profile configuration for Loki and Promtail

## Services

### Default Profile (without grafana)

Starts only:

- processor
- db (PostgreSQL)

Run:

```bash
docker compose up --build
```

### grafana Profile

Starts central Grafana with full provisioning (datasources and dashboard):

- grafana

Run:

```bash
docker compose --profile grafana up --build
```

### monitoring Profile

Starts technical monitoring containers only:

- loki
- promtail

Run:

```bash
docker compose --profile monitoring up --build
```

### Combined Profiles

For log queries in Grafana, run both profiles together:

```bash
docker compose --profile grafana --profile monitoring up --build
```

If only `grafana` is active, Loki-based queries are not available until `monitoring` is also active.

## Environment and Secrets

This repository is configured to avoid hard-coded secrets.

1. Copy `.env.example` to `.env`.
2. Set real credentials and sensitive values in `.env`.
3. Never commit `.env`.

The `.env.example` file contains all required variables, including:

- PostgreSQL credentials
- Processor database URL
- Grafana admin credentials
- Grafana default language

## Processor Notes

The processor container is prepared so setuptools_scm can work:

- git is installed in the container
- git core.autocrlf is globally enabled

Current entrypoint:

- Python module startup via `python -m processor`

## Documentation

| Document | Description |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Overall system architecture, service topology, ports, data flow, and volumes |
| [docs/PROCESSOR.md](docs/PROCESSOR.md) | Processor service internals: pipelines, matching logic, loading strategy, repository layer |
| [docs/DATABASE.md](docs/DATABASE.md) | Relational schema, table definitions, and field semantics |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Runtime configuration model and YAML reference |
| [docs/SCRIPTS.md](docs/SCRIPTS.md) | Helper scripts for database access and Grafana re-provisioning |
| [docs/pipelines/GTFS.md](docs/pipelines/GTFS.md) | GTFS nominal pipeline specification |
| [docs/pipelines/GTFSRT-TRIPUPDATES.md](docs/pipelines/GTFSRT-TRIPUPDATES.md) | GTFS-RT TripUpdates realtime pipeline specification |

## License

See [LICENSE.md](LICENSE.md).

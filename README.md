# timeline

Base Docker and service setup for the timeline project with:

- processor (Python service, prepared for ETL)
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

## License

See [LICENSE.md](LICENSE.md).

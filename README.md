# timeline

Base Docker and service setup for the timeline project with:

- processor (Python service, prepared for ETL)
- PostgreSQL
- optional observability stack via the grafana profile (Grafana, Loki, Promtail)

## Requirements

- Docker Desktop (including Docker Compose)

## Project Structure

- docker-compose.yml: Service orchestration
- processor/: Python project in src layout with pyproject.toml, Alembic, and setuptools_scm
- monitoring/: Configuration for Grafana, Loki, and Promtail

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

Extends the stack with:

- grafana
- loki
- promtail

Run:

```bash
docker compose --profile grafana up --build
```

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

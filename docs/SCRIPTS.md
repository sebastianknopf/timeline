# Scripts

This document describes the helper scripts in the `scripts/` directory.

For the overall system architecture see [ARCHITECTURE.md](ARCHITECTURE.md).
For the database schema see [DATABASE.md](DATABASE.md).

## dbcmd

Opens an interactive `psql` session against the running `timeline-db` PostgreSQL container, or executes a single SQL command non-interactively.

### Files

| File | Platform |
| --- | --- |
| `scripts/dbcmd.ps1` | Windows (PowerShell 5.1+) |
| `scripts/dbcmd.sh` | Linux / macOS (Bash) |

### Prerequisites

- Docker is installed and available on `PATH`.
- The `timeline-db` container is running (`docker compose up -d db`).
- A `.env` file exists in the repository root with `POSTGRES_USER`, `POSTGRES_DB`, and `POSTGRES_PASSWORD` set. Use `.env.example` as the template.

### How it works

1. The script locates the repository root relative to its own path and reads the `.env` file.
2. `POSTGRES_USER`, `POSTGRES_DB`, and `POSTGRES_PASSWORD` are parsed from `.env`.
3. The script verifies that the `timeline-db` container is in a running state and exits with an error if not.
4. `PGPASSWORD` is set in the environment so `psql` can authenticate without an interactive password prompt.
5. `docker exec` is used to run `psql` inside the `timeline-db` container, which avoids the need for a local `psql` installation on the host.
6. When a SQL string is passed as the first argument, `psql` runs it with `--command` and exits. When no argument is given, a full interactive `psql` session is opened.

### Usage

**Interactive session**

```bash
# Linux
./scripts/dbcmd.sh
```

```powershell
# Windows
.\scripts\dbcmd.ps1
```

**Single SQL command**

```bash
# Linux
./scripts/dbcmd.sh "SELECT count(*) FROM dim_trips;"
```

```powershell
# Windows
.\scripts\dbcmd.ps1 "SELECT count(*) FROM dim_trips;"
```

On Linux, make the script executable once before first use:

```bash
chmod +x scripts/dbcmd.sh
```

### Security note

`PGPASSWORD` is set as a process-environment variable scoped to the script execution and cleared after the `docker exec` call completes. The password is never written to a file or passed on the command line.

## grfupdate

Forces Grafana to reload all provisioning configuration at runtime without restarting the container.

### Background

Grafana reads provisioning files (`datasources.yml`, `dashboards.yml`) only once at container startup. By default Grafana also polls dashboard JSON files under `/var/lib/grafana/dashboards` on a configurable interval (`updateIntervalSeconds`). In this project that polling is **disabled** (`updateIntervalSeconds: 0`) so that manual edits made inside the Grafana UI are not overwritten by files on disk. All provisioning reloads must therefore be triggered explicitly via this script.

The Grafana HTTP API exposes dedicated reload endpoints for each provisioning type, which this script calls without requiring a container restart.

### Files

| File | Platform |
| --- | --- |
| `scripts/grfupdate.ps1` | Windows (PowerShell 5.1+) |
| `scripts/grfupdate.sh` | Linux / macOS (Bash) |

### Prerequisites

- Docker is installed and available on `PATH`.
- The Grafana service is running (`docker compose --profile grafana up -d`).
- A `.env` file exists in the repository root with `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` set.
- `curl` is available on `PATH` (Linux/macOS). PowerShell 5.1+ uses `Invoke-RestMethod` and has no additional dependency.
- Grafana is reachable on `http://localhost:3000` (the default published port).

### How it works

1. The script locates the repository root relative to its own path and reads the `.env` file.
2. `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` are parsed from `.env`.
3. `docker compose --profile grafana ps --status running grafana` is called to verify the Grafana service is active. The script exits with an error if it is not running.
4. Four HTTP `POST` requests are sent to the Grafana API using Basic authentication:
   - `/api/admin/provisioning/dashboards/reload` — reloads dashboard provider configuration and dashboard JSON files.
   - `/api/admin/provisioning/datasources/reload` — reloads datasource configuration.
   - `/api/admin/provisioning/plugins/reload` — reloads plugin provisioning.
   - `/api/admin/provisioning/alerting/reload` — reloads alerting configuration.
5. Endpoints that return HTTP 404 are skipped silently — this occurs when a provisioning type is not configured in the running Grafana instance.
6. Each response status is reported. If any call fails with a non-200/non-404 status, the script exits with a non-zero status and points to the Grafana container logs.

### Usage

```bash
# Linux
./scripts/grfupdate.sh
```

```powershell
# Windows
.\scripts\grfupdate.ps1
```

On Linux, make the script executable once before first use:

```bash
chmod +x scripts/grfupdate.sh
```

### Security note

Grafana admin credentials are read from `.env` and passed to the API via HTTP Basic Auth over the loopback interface (`localhost`). They are never written to a file or echoed to the terminal.

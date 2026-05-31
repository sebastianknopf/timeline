#!/usr/bin/env bash
# grfupdate — force Grafana to reload all provisioning configuration without a restart.
#
# Usage:
#   ./scripts/grfupdate.sh
#
# The script reads GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD from the repository-root
# .env file, verifies that the Grafana service in this Docker Compose project is running,
# then calls the Grafana HTTP API reload endpoints for both dashboard and datasource
# provisioning.
#
# The Grafana container must be reachable on http://localhost:3000 (the default published
# port from docker-compose.yml).
#
# All Grafana provisioning reload endpoints are called (dashboards, datasources, plugins,
# alerting). Endpoints that return HTTP 404 are skipped silently — this happens when a
# provisioning type is not configured in the running Grafana instance.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env file not found at '$ENV_FILE'." >&2
    echo "Copy .env.example to .env and fill in your values." >&2
    exit 1
fi

parse_env() {
    local key="$1"
    grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d'=' -f2-
}

GF_USER="$(parse_env GRAFANA_ADMIN_USER)"
GF_PASS="$(parse_env GRAFANA_ADMIN_PASSWORD)"
GF_PORT="$(parse_env GRAFANA_HOST_PORT)"
GF_PORT="${GF_PORT:-3000}"

if [[ -z "$GF_USER" || -z "$GF_PASS" ]]; then
    echo "ERROR: GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD must both be set in .env." >&2
    exit 1
fi

# Check that the Grafana service is running inside this Compose project.
cd "$REPO_ROOT"
PS_OUTPUT="$(docker compose --profile grafana ps --status running grafana 2>&1 || true)"
if ! echo "$PS_OUTPUT" | grep -q "grafana"; then
    echo "ERROR: The Grafana service is not running." >&2
    echo "Start it with: docker compose --profile grafana up -d" >&2
    exit 1
fi

BASE_URL="http://localhost:${GF_PORT}"
ALL_OK=true

reload_endpoint() {
    local path="$1"
    local label="$2"
    local http_code
    http_code="$(curl --silent --output /dev/null --write-out "%{http_code}" \
        --request POST \
        --user "${GF_USER}:${GF_PASS}" \
        --header "Content-Type: application/json" \
        "${BASE_URL}${path}")"
    if [[ "$http_code" == "200" ]]; then
        echo "OK   ${label} reloaded."
    elif [[ "$http_code" == "404" ]]; then
        # Provisioning type not configured in this Grafana instance — skip silently.
        echo "SKIP ${label} (not configured)."
    else
        echo "FAIL ${label}: HTTP ${http_code}" >&2
        ALL_OK=false
    fi
}

reload_endpoint "/api/admin/provisioning/dashboards/reload"  "dashboard provisioning"
reload_endpoint "/api/admin/provisioning/datasources/reload" "datasource provisioning"
reload_endpoint "/api/admin/provisioning/plugins/reload"     "plugin provisioning"
reload_endpoint "/api/admin/provisioning/alerting/reload"    "alerting provisioning"

if [[ "$ALL_OK" != "true" ]]; then
    echo "One or more reload calls failed. Check the Grafana logs:" >&2
    echo "  docker compose --profile grafana logs grafana" >&2
    exit 1
fi

echo "Grafana provisioning reload complete."

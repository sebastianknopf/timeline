#!/usr/bin/env bash
# dbcmd — open an interactive psql session against the Timeline PostgreSQL container.
#
# Usage:
#   ./scripts/dbcmd.sh                              # interactive session
#   ./scripts/dbcmd.sh "SELECT count(*) FROM dim_trips;"  # single command, then exit
#
# The script reads POSTGRES_USER, POSTGRES_DB, and POSTGRES_PASSWORD from the
# .env file in the repository root and connects to the running timeline-db
# container via docker exec.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$REPO_ROOT/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env file not found at '$ENV_FILE'." >&2
    echo "Copy .env.example to .env and fill in your values." >&2
    exit 1
fi

# Parse key=value pairs from .env; ignore blank lines and comments.
parse_env() {
    local key="$1"
    grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d'=' -f2-
}

DB_USER="$(parse_env POSTGRES_USER)"
DB_NAME="$(parse_env POSTGRES_DB)"
DB_PASS="$(parse_env POSTGRES_PASSWORD)"

if [[ -z "$DB_USER" || -z "$DB_NAME" || -z "$DB_PASS" ]]; then
    echo "ERROR: POSTGRES_USER, POSTGRES_DB, and POSTGRES_PASSWORD must all be set in .env." >&2
    exit 1
fi

CONTAINER_NAME="timeline-db"

# Verify the container is running before attempting to connect.
RUNNING="$(docker inspect --format '{{.State.Running}}' "$CONTAINER_NAME" 2>/dev/null || true)"
if [[ "$RUNNING" != "true" ]]; then
    echo "ERROR: Container '$CONTAINER_NAME' is not running." >&2
    echo "Start it with: docker compose up -d db" >&2
    exit 1
fi

export PGPASSWORD="$DB_PASS"

if [[ $# -gt 0 ]]; then
    # Non-interactive: execute a single SQL command and exit.
    docker exec -i "$CONTAINER_NAME" \
        psql --username="$DB_USER" --dbname="$DB_NAME" --command="$1"
else
    # Interactive: open a full psql terminal session.
    docker exec -it "$CONTAINER_NAME" \
        psql --username="$DB_USER" --dbname="$DB_NAME"
fi

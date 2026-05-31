#Requires -Version 5.1
<#
.SYNOPSIS
    Open an interactive psql session against the Timeline PostgreSQL container.

.DESCRIPTION
    Reads POSTGRES_USER, POSTGRES_DB, and POSTGRES_PASSWORD from the .env file
    located in the repository root, then connects to the running timeline-db
    container via docker exec.

    When no argument is given the script drops into an interactive psql session.
    When a SQL string is supplied as the first positional argument it is executed
    as a single command and the session exits immediately.

.PARAMETER Command
    Optional SQL command to execute non-interactively.

.EXAMPLE
    # Interactive session
    .\scripts\dbcmd.ps1

.EXAMPLE
    # Single command
    .\scripts\dbcmd.ps1 "SELECT count(*) FROM dim_trips;"
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Command
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Resolve the repository root relative to this script's location.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$EnvFile  = Join-Path $RepoRoot ".env"

if (-not (Test-Path $EnvFile)) {
    Write-Error ".env file not found at '$EnvFile'. Copy .env.example to .env and fill in your values."
}

# Parse key=value pairs from .env; ignore blank lines and comments.
$EnvVars = @{}
foreach ($Line in Get-Content $EnvFile) {
    $Line = $Line.Trim()
    if ($Line -eq "" -or $Line.StartsWith("#")) { continue }
    $Index = $Line.IndexOf("=")
    if ($Index -lt 1) { continue }
    $Key   = $Line.Substring(0, $Index).Trim()
    $Value = $Line.Substring($Index + 1).Trim()
    $EnvVars[$Key] = $Value
}

$DbUser = $EnvVars["POSTGRES_USER"]
$DbName = $EnvVars["POSTGRES_DB"]
$DbPass = $EnvVars["POSTGRES_PASSWORD"]

if (-not $DbUser -or -not $DbName -or -not $DbPass) {
    Write-Error "POSTGRES_USER, POSTGRES_DB, and POSTGRES_PASSWORD must all be set in .env."
}

$ContainerName = "timeline-db"

# Verify the container is running before attempting to connect.
$Status = docker inspect --format "{{.State.Running}}" $ContainerName 2>$null
if ($Status -ne "true") {
    Write-Error "Container '$ContainerName' is not running. Start it with: docker compose up -d db"
}

if ($Command) {
    # Non-interactive: execute a single SQL command and exit.
    $env:PGPASSWORD = $DbPass
    docker exec -i $ContainerName psql --username=$DbUser --dbname=$DbName --command=$Command
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
} else {
    # Interactive: open a full psql terminal session.
    $env:PGPASSWORD = $DbPass
    docker exec -it $ContainerName psql --username=$DbUser --dbname=$DbName
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}

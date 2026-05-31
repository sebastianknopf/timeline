#Requires -Version 5.1
<#
.SYNOPSIS
    Force Grafana to reload all provisioning configuration without a container restart.

.DESCRIPTION
    Reads GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD from the repository-root .env
    file, verifies that the Grafana service in this Docker Compose project is running,
    then calls all Grafana HTTP API provisioning reload endpoints (dashboards, datasources,
    plugins, alerting). Endpoints that return HTTP 404 are skipped silently, which happens
    when a provisioning type is not configured in this Grafana instance.

    The Grafana container must be reachable on http://localhost:3000 (the default
    published port from docker-compose.yml).

.EXAMPLE
    .\scripts\grfupdate.ps1
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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

$GfUser = $EnvVars["GRAFANA_ADMIN_USER"]
$GfPass = $EnvVars["GRAFANA_ADMIN_PASSWORD"]

if (-not $GfUser -or -not $GfPass) {
    Write-Error "GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD must both be set in .env."
}

# Check that the Grafana service is running inside this Compose project.
# docker compose ps prints one line per service replica; filter for grafana and running state.
Push-Location $RepoRoot
try {
    $PsOutput = docker compose --profile grafana ps --status running grafana 2>&1 | Out-String
} finally {
    Pop-Location
}

if ($PsOutput -notmatch "grafana") {
    Write-Error "The Grafana service is not running. Start it with: docker compose --profile grafana up -d"
}

$BaseUrl   = "http://localhost:3000"
$AuthBytes = [System.Text.Encoding]::UTF8.GetBytes("${GfUser}:${GfPass}")
$AuthB64   = [Convert]::ToBase64String($AuthBytes)
$Headers   = @{ Authorization = "Basic $AuthB64"; "Content-Type" = "application/json" }

$Endpoints = @(
    @{ Path = "/api/admin/provisioning/dashboards/reload";   Label = "dashboard provisioning" },
    @{ Path = "/api/admin/provisioning/datasources/reload";  Label = "datasource provisioning" },
    @{ Path = "/api/admin/provisioning/plugins/reload";      Label = "plugin provisioning" },
    @{ Path = "/api/admin/provisioning/alerting/reload";     Label = "alerting provisioning" }
)

$AllOk = $true
foreach ($Ep in $Endpoints) {
    $Url = "$BaseUrl$($Ep.Path)"
    try {
        $Response = Invoke-RestMethod -Method Post -Uri $Url -Headers $Headers
        Write-Host "OK   $($Ep.Label) reloaded."
    } catch {
        # HTTP 404 means the provisioning type is not configured — skip silently.
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode -eq 404) {
            Write-Host "SKIP $($Ep.Label) (not configured)."
        } else {
            Write-Warning "FAIL $($Ep.Label): $_"
            $AllOk = $false
        }
    }
}

if (-not $AllOk) {
    Write-Error "One or more reload calls failed. Check the Grafana logs: docker compose --profile grafana logs grafana"
}

Write-Host "Grafana provisioning reload complete."

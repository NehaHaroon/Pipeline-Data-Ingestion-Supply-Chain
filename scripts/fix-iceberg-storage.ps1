# Fix Iceberg catalog lock permission errors (Airflow compaction / transformation).
# Run from repo root. Stops writers, removes stale locks, optionally fixes ownership via Docker.

param(
    [switch]$DockerChown,
    [int]$Uid = $(if ($env:AIRFLOW_UID) { [int]$env:AIRFLOW_UID } else { 50000 })
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "Removing stale Iceberg lock files under storage/ ..."
$patterns = @(
    "iceberg_catalog.db.session.lock",
    ".iceberg_pyiceberg_catalog.lock",
    "storage\.locks\*"
)
foreach ($name in @("iceberg_catalog.db.session.lock", ".iceberg_pyiceberg_catalog.lock")) {
    $p = Join-Path $root "storage" $name
    if (Test-Path $p) { Remove-Item -Force $p; Write-Host "  deleted $p" }
}
$locksDir = Join-Path $root "storage" ".locks"
if (Test-Path $locksDir) {
    Remove-Item -Recurse -Force $locksDir
    Write-Host "  deleted $locksDir"
}

if ($DockerChown) {
    Write-Host "Chown storage to UID $Uid inside Airflow image (requires docker-compose.airflow.yml) ..."
    docker compose -f docker-compose.airflow.yml run --rm --user "0:0" airflow-init bash -c @"
set -e
chown -R ${Uid}:0 /opt/airflow/project/storage /opt/airflow/storage 2>/dev/null || true
chmod -R u+rwX,g+rwX /opt/airflow/project/storage 2>/dev/null || true
echo storage permissions updated for UID ${Uid}
"@
}

Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Ensure .env has AIRFLOW_UID=$Uid and PIPELINE_UID=$Uid (same value)."
Write-Host "  2. docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d --build"
Write-Host "  3. Retry compaction DAG after ingestion/transform are idle."

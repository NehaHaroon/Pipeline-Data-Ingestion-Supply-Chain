<#
.SYNOPSIS
Starts Docker Compose with automatic PostgreSQL host-port fallback.

.DESCRIPTION
If host port 5432 is occupied, this script switches to 5433 automatically.
You can still override explicitly by setting POSTGRES_HOST_PORT before running.
#>

param(
    [string]$ProjectRoot = "."
)

$ErrorActionPreference = "Stop"

if (-not $env:POSTGRES_HOST_PORT -or [string]::IsNullOrWhiteSpace($env:POSTGRES_HOST_PORT)) {
    $primaryPort = 5432
    $fallbackPort = 5433

    $portInUse = Get-NetTCPConnection -State Listen -LocalPort $primaryPort -ErrorAction SilentlyContinue
    if ($portInUse) {
        $env:POSTGRES_HOST_PORT = "$fallbackPort"
        Write-Host "[STACK][WARN] Port $primaryPort is occupied; using host port $fallbackPort for PostgreSQL."
    }
    else {
        $env:POSTGRES_HOST_PORT = "$primaryPort"
        Write-Host "[STACK] Using default PostgreSQL host port $primaryPort."
    }
}
else {
    Write-Host "[STACK] Using POSTGRES_HOST_PORT from environment: $($env:POSTGRES_HOST_PORT)"
}

Push-Location $ProjectRoot
try {
    docker compose up -d --build
}
finally {
    Pop-Location
}

# Install Part 3 WITHOUT changing Part 1/2 dependency pins (requirements.txt untouched).
#
# Default (safe): only dbt — use when pipeline deps already work
#   powershell -ExecutionPolicy Bypass -File scripts/install-part3.ps1
#
# Full Part 3 venv from scratch:
#   powershell -ExecutionPolicy Bypass -File scripts/install-part3.ps1 -Full

param(
    [switch]$Full
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$py = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
Write-Host "Python version: $py"

if ($Full) {
    if ($py -notin @("3.11", "3.12")) {
        Write-Warning "Python $py may fail on pyiceberg/pyarrow builds. Prefer 3.11 or Docker."
    }
    Write-Host "Full install: requirements.txt + Part 3 extras (with constraints)..."
    python -m pip install --upgrade pip "setuptools>=69,<81" wheel
    python -m pip install --no-cache-dir -r requirements-part3.txt -c constraints-part3.txt
} else {
    Write-Host "Safe install: Part 3 extras only (dbt) — does not reinstall pyiceberg/pyarrow..."
    python -m pip install --upgrade pip
    python -m pip install --no-cache-dir -r requirements-part3-extras.txt
}

Write-Host "Verify Part 3:"
python -c "import dbt; print('dbt OK')"
python -c "import duckdb; print('duckdb OK')"
Write-Host "Done. Ingestion/transformation requirements.txt was not modified."

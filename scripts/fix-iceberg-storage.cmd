@echo off
REM Fix storage without PowerShell scripts (group-policy safe). Run from repo root.
cd /d "%~dp0.."

if not exist "storage\analytics" mkdir "storage\analytics"

del /f /q "storage\analytics\query_audit.jsonl" 2>nul
del /f /q "storage\analytics\metrics_state.json" 2>nul
del /f /q "storage\telemetry\telemetry_summary.json" 2>nul
del /f /q "storage\iceberg_catalog.db.session.lock" 2>nul
del /f /q "storage\.iceberg_pyiceberg_catalog.lock" 2>nul
rmdir /s /q "storage\.locks" 2>nul

echo.
echo Local files cleared. Optional Docker fix:
echo   python scripts\fix_iceberg_storage.py
echo   docker compose -f docker-compose.part3.yml run --rm --user 0:0 --entrypoint bash analytics-service -c "chmod -R a+rwX /app/storage/analytics"
echo.

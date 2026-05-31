@echo off
REM Full pipeline restart: Docker stacks + Airflow DAGs + Part 3 BI refresh
REM Run from repo root in CMD (no PowerShell required).
setlocal EnableDelayedExpansion
cd /d "%~dp0.."

echo ============================================================
echo  Supply Chain Pipeline - FULL RESTART
echo ============================================================
echo.

echo [1/9] Stopping all stacks...
docker compose -f docker-compose.part3.yml down 2>nul
docker compose -f docker-compose.airflow.yml down 2>nul
docker compose -f docker-compose.yml down 2>nul
timeout /t 5 /nobreak >nul

echo [2/9] Cleaning local lock + analytics files...
if not exist "storage\analytics" mkdir "storage\analytics"
if not exist "storage\telemetry" mkdir "storage\telemetry"
if not exist "logs" mkdir "logs"
del /f /q "storage\analytics\query_audit.jsonl" 2>nul
del /f /q "storage\analytics\metrics_state.json" 2>nul
del /f /q "storage\telemetry\telemetry_summary.json" 2>nul
del /f /q "storage\iceberg_catalog.db.session.lock" 2>nul
del /f /q "storage\.iceberg_pyiceberg_catalog.lock" 2>nul
rmdir /s /q "storage\.locks" 2>nul

echo [3/9] Starting Part 1 - Ingestion + Kafka + Prometheus...
docker compose -f docker-compose.yml up -d --build
if errorlevel 1 goto :fail
call :wait_http http://localhost:8000/health 120 "ingestion-api"

echo [4/9] Airflow Postgres + DB init (required before scheduler)...
docker compose -f docker-compose.airflow.yml up -d airflow-postgres
if errorlevel 1 goto :fail
call :wait_pg 90
docker compose -f docker-compose.airflow.yml build airflow-init
docker compose -f docker-compose.airflow.yml run --rm airflow-init
if errorlevel 1 (
  echo.
  echo ERROR: airflow-init failed. Check output above.
  echo Try: docker compose -f docker-compose.airflow.yml logs airflow-init
  goto :fail
)

echo [5/9] Starting Airflow scheduler, webserver, transform-service...
docker compose -f docker-compose.airflow.yml up -d --build airflow-scheduler airflow-webserver transform-service
if errorlevel 1 goto :fail
call :wait_airflow_scheduler 180
call :wait_http http://localhost:8080/health 120 "airflow-webserver"
call :wait_http http://localhost:8001/docs 120 "transform-service"

echo [6/9] Starting Part 3 - analytics-service + Grafana...
docker compose -f docker-compose.part3.yml up -d --build
if errorlevel 1 goto :fail
call :wait_http http://localhost:8002/health 120 "analytics-service"

echo [7/9] Fixing storage permissions in containers...
docker compose -f docker-compose.part3.yml run --rm --user 0:0 --entrypoint bash analytics-service -c "mkdir -p /app/storage/analytics && chmod -R a+rwX /app/storage/analytics" 2>nul
docker compose -f docker-compose.yml run --rm --user 0:0 --entrypoint bash ingestion-api -c "mkdir -p /app/storage/telemetry /app/storage/analytics && chmod -R a+rwX /app/storage/telemetry /app/storage/analytics" 2>nul
docker compose -f docker-compose.airflow.yml run --rm --user 0:0 --entrypoint bash airflow-scheduler -c "chmod -R a+rwX /opt/airflow/logs" 2>nul

echo [8/9] Triggering Airflow DAGs (watch http://localhost:8080 - admin/admin)...
call :airflow_db_check
if errorlevel 1 goto :fail
call :trigger_dag supply_chain_ingestion
echo   Waiting 3 min for ingestion...
timeout /t 180 /nobreak >nul

call :trigger_dag supply_chain_transformation
echo   Waiting 5 min for Silver/Gold transform...
timeout /t 300 /nobreak >nul

call :trigger_dag supply_chain_iceberg_compaction
echo   Waiting 2 min for compaction...
timeout /t 120 /nobreak >nul

call :trigger_dag supply_chain_semantic
echo   Waiting 3 min for semantic export + SQL...
timeout /t 180 /nobreak >nul

echo [9/9] Refreshing executive BI metrics...
curl -s -X POST http://localhost:8002/semantic/refresh-bi-metrics
echo   Waiting 90s for BI refresh...
timeout /t 90 /nobreak >nul
curl -s http://localhost:8002/semantic/executive-dashboard > storage\analytics\executive_dashboard.json 2>nul
curl -s http://localhost:8002/metrics/prometheus >nul 2>&1

echo.
echo ============================================================
echo  DONE - Open these URLs:
echo   Airflow:     http://localhost:8080  (admin / admin)
echo   Grafana BI:  http://localhost:3000/d/supply-chain-bi  (admin / admin)
echo   Executive:   http://localhost:8002/dashboard
echo   Prometheus: http://localhost:9090/targets
echo.
echo  Verify: curl http://localhost:8002/semantic/prometheus-bi-catalog
echo ============================================================
goto :eof

:airflow_db_check
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow db check
if errorlevel 1 (
  echo   ERROR: Airflow database not initialized. Re-run:
  echo   docker compose -f docker-compose.airflow.yml run --rm airflow-init
  exit /b 1
)
echo   Airflow DB check OK
exit /b 0

:trigger_dag
call :airflow_db_check
if errorlevel 1 exit /b 1
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow dags trigger %1
if errorlevel 1 (
  echo   FAILED to trigger %1
  exit /b 1
)
echo   Triggered DAG: %1
exit /b 0

:wait_pg
set /a N=0
:wait_pg_loop
docker compose -f docker-compose.airflow.yml exec -T airflow-postgres pg_isready -U airflow >nul 2>&1
if not errorlevel 1 goto :wait_pg_ok
set /a N+=5
if !N! GEQ %1 (
  echo   WARNING: postgres slow to start
  goto :eof
)
timeout /t 5 /nobreak >nul
goto :wait_pg_loop
:wait_pg_ok
echo   airflow-postgres is ready
goto :eof

:wait_airflow_scheduler
set /a N=0
:wait_sched_loop
docker compose -f docker-compose.airflow.yml ps airflow-scheduler 2>nul | findstr /i "running" >nul
if not errorlevel 1 (
  docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow db check >nul 2>&1
  if not errorlevel 1 goto :wait_sched_ok
)
set /a N+=10
if !N! GEQ %1 (
  echo   WARNING: scheduler not stable - check: docker compose -f docker-compose.airflow.yml logs airflow-scheduler
  goto :eof
)
echo   Waiting for Airflow scheduler...
timeout /t 10 /nobreak >nul
goto :wait_sched_loop
:wait_sched_ok
echo   airflow-scheduler is running and DB is OK
goto :eof

:wait_http
set URL=%1
set SEC=%2
set NAME=%3
set /a N=0
:wait_loop
curl -s -o nul -w "%%{http_code}" "%URL%" 2>nul | findstr /r "^200$" >nul && goto :wait_ok
set /a N+=5
if !N! GEQ %SEC% (
  echo   WARNING: %NAME% not healthy at %URL% after %SEC%s - continuing anyway
  goto :eof
)
timeout /t 5 /nobreak >nul
goto :wait_loop
:wait_ok
echo   %NAME% is up
goto :eof

:fail
echo.
echo FAILED - see messages above.
exit /b 1

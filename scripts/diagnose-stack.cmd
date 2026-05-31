@echo off
REM Quick health check — finds which service is down and why.
REM Run from repo root in CMD.
setlocal EnableDelayedExpansion
cd /d "%~dp0.."

echo ============================================================
echo  PIPELINE STACK DIAGNOSTICS
echo  %DATE% %TIME%
echo ============================================================
echo.

echo --- Docker engine ---
docker info >nul 2>&1
if errorlevel 1 (
  echo [FAIL] Docker is not running or not reachable.
  echo        Start Docker Desktop, wait until it is green, then re-run this script.
  goto :eof
)
echo [OK] Docker engine is reachable
echo.

echo --- Network pipeline-net ---
docker network inspect pipeline-net >nul 2>&1
if errorlevel 1 (
  echo [FAIL] pipeline-net missing. Create it:
  echo        docker network create pipeline-net
) else (
  echo [OK] pipeline-net exists
)
echo.

echo --- All project containers (name / status / ports) ---
docker ps -a --filter network=pipeline-net --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo.

echo --- Expected host ports ---
echo   8000  ingestion-api      (Part 1)
echo   8080  Airflow webserver  (Part 2)  ^<- NOT 8000
echo   8001  transform-service
echo   8002  analytics-service
echo   3000  Grafana
echo   9090  Prometheus
echo.

echo --- HTTP probes from host (localhost) ---
call :probe http://localhost:8000/health        "ingestion-api :8000"
call :probe http://localhost:8080/health        "Airflow       :8080"
call :probe http://localhost:8001/health        "transform     :8001"
call :probe http://localhost:8002/health        "analytics     :8002"
call :probe http://localhost:3000/api/health    "Grafana       :3000"
call :probe http://localhost:9090/-/healthy     "Prometheus    :9090"
echo.
echo.
echo --- Docker port mappings (verify host ports are published) ---
docker port pipeline-data-ingestion-supply-chain-ingestion-api-1 8000 2>nul
docker port pipeline-prometheus 9090 2>nul
docker port airflow-webserver 8080 2>nul
docker port transform-service 8001 2>nul
docker port analytics-service 8002 2>nul
docker port grafana-part3 3000 2>nul
echo.

echo --- Exited or restarting containers (likely root cause) ---
set FOUND=0
for /f "tokens=*" %%L in ('docker ps -a --filter network=pipeline-net --filter "status=exited" --format "{{.Names}}|{{.Status}}" 2^>nul') do (
  set FOUND=1
  for /f "tokens=1,2 delims=|" %%A in ("%%L") do echo [EXITED] %%A — %%B
)
for /f "tokens=*" %%L in ('docker ps -a --filter network=pipeline-net --filter "status=restarting" --format "{{.Names}}|{{.Status}}" 2^>nul') do (
  set FOUND=1
  for /f "tokens=1,2 delims=|" %%A in ("%%L") do echo [RESTARTING] %%A — %%B
)
if !FOUND!==0 echo   (none — check logs for running-but-unhealthy services)
echo.

echo --- Dependency chain checks ---
call :check_container kafka
call :check_container ingestion-api
call :check_container airflow-postgres
call :check_container airflow-scheduler
call :check_container airflow-webserver
call :check_container transform-service
call :check_container analytics-service
call :check_container grafana-part3
call :check_container pipeline-prometheus
echo.

echo --- Last 15 log lines for key services (errors) ---
call :logs ingestion-api
call :logs airflow-webserver
call :logs airflow-scheduler
call :logs kafka
call :logs transform-service
call :logs analytics-service
echo.

echo --- airfow-init one-shot status (must have completed for webserver) ---
docker compose -f docker-compose.airflow.yml ps airflow-init 2>nul
echo.

echo ============================================================
echo  COMMON FIXES
echo ============================================================
echo  1. Nothing on :8000 — start Part 1 and wait for Kafka:
echo     docker compose -f docker-compose.yml up -d --build
echo     docker compose -f docker-compose.yml logs kafka ingestion-api --tail 50
echo.
echo  2. Airflow not on :8080 — init must succeed first:
echo     docker compose -f docker-compose.airflow.yml up -d airflow-postgres
echo     docker compose -f docker-compose.airflow.yml run --rm airflow-init
echo     docker compose -f docker-compose.airflow.yml up -d airflow-scheduler airflow-webserver
echo     docker compose -f docker-compose.airflow.yml logs airflow-webserver --tail 50
echo.
echo  3. Full rebuild:
echo     scripts\rebuild-all.cmd
echo ============================================================
goto :eof

:probe
set URL=%1
set LABEL=%~2
for /f %%C in ('curl -s -o nul -w "%%{http_code}" "%URL%" 2^>nul') do set CODE=%%C
if "!CODE!"=="200" (
  echo [OK]   !LABEL!  !URL!  HTTP=!CODE!
) else (
  echo [FAIL] !LABEL!  !URL!  HTTP=!CODE!  ^(try: curl -v "!URL!"^)
)
goto :eof

:check_container
set C=%1
docker ps --filter "name=%C%" --filter "status=running" -q 2>nul | findstr /r "." >nul
if not errorlevel 1 (
  echo [RUNNING] %C%
) else (
  docker ps -a --filter "name=%C%" -q 2>nul | findstr /r "." >nul
  if errorlevel 1 (
    echo [MISSING] %C% — not created; run docker compose up for that stack
  ) else (
    for /f "tokens=*" %%L in ('docker ps -a --filter "name=%C%" --format "{{.Names}} | {{.Status}}" 2^>nul') do echo [DOWN] %%L
  )
)
goto :eof

:logs
set SVC=%1
echo.
echo === %SVC% ===
docker compose -f docker-compose.yml logs %SVC% --tail 15 2>nul
docker compose -f docker-compose.airflow.yml logs %SVC% --tail 15 2>nul
docker compose -f docker-compose.part3.yml logs %SVC% --tail 15 2>nul
docker logs %SVC% --tail 15 2>nul
goto :eof

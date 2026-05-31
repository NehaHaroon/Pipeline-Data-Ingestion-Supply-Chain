@echo off
REM Rebuild all custom images and start every stack (Part 1 + Airflow + Part 3).
REM Run from repo root in CMD.
setlocal EnableDelayedExpansion
cd /d "%~dp0.."

echo ============================================================
echo  Rebuild ALL images and start containers
echo ============================================================
echo.

echo [1/7] Ensuring Docker network pipeline-net...
docker network create pipeline-net 2>nul

echo [2/7] Part 1 - build + start (Kafka, ingestion-api, Prometheus, ...)...
docker compose -f docker-compose.yml up -d --build
if errorlevel 1 goto :fail
call :wait_http http://localhost:8000/health 120 "ingestion-api"

echo [3/7] Airflow Postgres...
docker compose -f docker-compose.airflow.yml up -d airflow-postgres
if errorlevel 1 goto :fail
call :wait_pg 90

echo [4/7] Airflow init (db migrate + admin user)...
docker compose -f docker-compose.airflow.yml build airflow-init
docker compose -f docker-compose.airflow.yml run --rm airflow-init
if errorlevel 1 (
  echo ERROR: airflow-init failed
  goto :fail
)

echo [5/7] Part 2 - build + start Airflow + transform-service...
docker compose -f docker-compose.airflow.yml up -d --build airflow-scheduler airflow-webserver transform-service
if errorlevel 1 goto :fail
call :wait_http http://localhost:8080/health 180 "airflow-webserver"
call :wait_http http://localhost:8001/docs 120 "transform-service"

echo [6/7] Part 3 - build + start analytics-service + Grafana...
docker compose -f docker-compose.part3.yml up -d --build analytics-service grafana
if errorlevel 1 goto :fail
call :wait_http http://localhost:8002/health 120 "analytics-service"
call :wait_http http://localhost:3000/api/health 120 "grafana"

echo.
echo Refreshing BI metrics for Grafana...
curl -s -X POST http://localhost:8002/semantic/refresh-bi-metrics
echo.

echo.
echo ============================================================
echo  ALL STACKS UP:
echo   http://localhost:8000/dashboard
echo   http://localhost:8080        Airflow (admin / admin)
echo   http://localhost:8001/docs
echo   http://localhost:8002/dashboard
echo   http://localhost:3000/d/supply-chain-bi
echo   http://localhost:9090/targets
echo ============================================================
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
goto :eof

:wait_pg
set /a N=0
:wait_pg_loop
docker compose -f docker-compose.airflow.yml exec -T airflow-postgres pg_isready -U airflow >nul 2>&1
if not errorlevel 1 goto :wait_pg_ok
set /a N+=5
if !N! GEQ %1 goto :wait_pg_ok
timeout /t 5 /nobreak >nul
goto :wait_pg_loop
:wait_pg_ok
echo   airflow-postgres is ready
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
  echo   WARNING: %NAME% not healthy at %URL% after %SEC%s - continuing
  goto :eof
)
timeout /t 5 /nobreak >nul
goto :wait_loop
:wait_ok
echo   %NAME% is up
goto :eof

:fail
echo.
echo FAILED - check docker compose logs for the failing service.
exit /b 1

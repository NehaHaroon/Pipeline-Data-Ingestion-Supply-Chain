@echo off
REM Start Part 2 (Airflow + transform) and Part 3 (analytics + Grafana).
setlocal EnableDelayedExpansion
cd /d "%~dp0.."

echo ============================================================
echo  Start Part 2 + Part 3
echo ============================================================

docker network create pipeline-net 2>nul

echo.
echo [1/4] Airflow Postgres...
docker compose -f docker-compose.airflow.yml up -d airflow-postgres
if errorlevel 1 goto :fail
call :wait_pg 90

echo.
echo [2/4] Airflow init (db migrate + admin user)...
docker compose -f docker-compose.airflow.yml run --rm airflow-init
if errorlevel 1 (
  echo.
  echo ERROR: airflow-init failed. Logs:
  docker compose -f docker-compose.airflow.yml logs airflow-init --tail 40
  goto :fail
)

echo.
echo [3/4] Airflow scheduler/webserver + transform-service...
docker compose -f docker-compose.airflow.yml up -d --build airflow-scheduler airflow-webserver transform-service
if errorlevel 1 goto :fail
call :wait_http http://localhost:8080/health 180 "Airflow"
call :wait_http http://localhost:8001/health 120 "transform-service"

echo.
echo [4/4] analytics-service + Grafana...
docker compose -f docker-compose.part3.yml up -d --build analytics-service grafana
if errorlevel 1 goto :fail
call :wait_http http://localhost:8002/health 120 "analytics-service"
call :wait_http http://localhost:3000/api/health 120 "Grafana"

echo.
echo Refreshing BI metrics...
curl -s -X POST http://localhost:8002/semantic/refresh-bi-metrics
echo.

echo.
echo ============================================================
echo  DONE — try these in browser:
echo   http://localhost:8000/health
echo   http://localhost:8080        Airflow (admin / admin)
echo   http://localhost:8001/docs
echo   http://localhost:8002/health
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
  echo   WARNING: %NAME% not responding at %URL% after %SEC%s
  goto :eof
)
timeout /t 5 /nobreak >nul
goto :wait_loop
:wait_ok
echo   %NAME% is up at %URL%
goto :eof

:fail
echo FAILED.
exit /b 1

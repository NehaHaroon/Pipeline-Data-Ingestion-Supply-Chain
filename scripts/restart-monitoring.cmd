@echo off
REM Restart Prometheus + analytics-service + Grafana (Part 1 + Part 3 only).
REM Use when Grafana shows no data or run-all returns ECONNRESET.
setlocal EnableDelayedExpansion
cd /d "%~dp0.."

echo ============================================================
echo  Restart monitoring stack (Prometheus + Analytics + Grafana)
echo ============================================================
echo.

echo [1/5] Restarting Part 1 Prometheus...
docker compose -f docker-compose.yml up -d prometheus
if errorlevel 1 goto :fail
call :wait_http http://localhost:9090/-/healthy 60 "prometheus"

echo [2/5] Rebuilding and restarting Part 3 analytics + Grafana...
docker compose -f docker-compose.part3.yml up -d --build analytics-service grafana
if errorlevel 1 goto :fail
call :wait_http http://localhost:8002/health 120 "analytics-service"
call :wait_http http://localhost:3000/api/health 120 "grafana"

echo [3/5] Lightweight BI metrics refresh (Grafana panels)...
curl -s -X POST http://localhost:8002/semantic/refresh-bi-metrics
echo.

echo [4/5] Waiting 90s for BI refresh to finish...
timeout /t 90 /nobreak >nul

echo [5/5] Verify metrics...
curl -s http://localhost:8002/metrics/prometheus | findstr /i "analytics_bi_metric warehouse_units" >nul
if errorlevel 1 (
  echo   WARNING: No labeled BI metrics yet — run full SQL workloads when ready:
  echo   curl -X POST http://localhost:8002/analytics/run-all
) else (
  echo   BI metrics present on analytics-service
)

echo.
echo ============================================================
echo  DONE
echo   Grafana:     http://localhost:3000/d/supply-chain-bi
echo   Prometheus:  http://localhost:9090/targets
echo   Analytics:   http://localhost:8002/health
echo.
echo  For full SQL refresh (heavy, may take several minutes):
echo   curl -X POST http://localhost:8002/analytics/run-all
echo ============================================================
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
  echo   WARNING: %NAME% not healthy at %URL% after %SEC%s
  goto :eof
)
timeout /t 5 /nobreak >nul
goto :wait_loop
:wait_ok
echo   %NAME% is up
goto :eof

:fail
echo FAILED - see messages above.
exit /b 1

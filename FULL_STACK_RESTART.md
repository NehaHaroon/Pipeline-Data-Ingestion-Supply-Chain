# Full Stack Restart — View Correct Dashboard Data

Use this when Grafana shows **No data**, SQL workloads fail, or after pulling new code.

**No PowerShell required** — use **CMD** only.

---

## Quick restart (one command)

Open **Command Prompt**, `cd` to the repo root, run:

```cmd
scripts\restart-full-stack.cmd
```

Takes about **20–25 minutes** (includes wait times for DAGs). Watch progress in Airflow: http://localhost:8080 (login `admin` / `admin`).

---

## Manual step-by-step (if the script fails)

### 1. Stop everything

```cmd
cd C:\OneDrive - TPS Pakistan (Pvt.) Ltd\Documents\GitHub\Pipeline-Data-Ingestion-Supply-Chain

docker compose -f docker-compose.part3.yml down
docker compose -f docker-compose.airflow.yml down
docker compose -f docker-compose.yml down
```

### 2. Clean locks / analytics (CMD)

```cmd
scripts\fix-iceberg-storage.cmd
```

Or:

```cmd
    python scripts\fix_iceberg_storage.py
```

### 3. Start in order (wait ~1–2 min between each)

```cmd
docker compose -f docker-compose.yml up -d --build

docker compose -f docker-compose.airflow.yml up -d airflow-postgres
docker compose -f docker-compose.airflow.yml run --rm airflow-init
docker compose -f docker-compose.airflow.yml up -d airflow-scheduler airflow-webserver transform-service

docker compose -f docker-compose.part3.yml up -d --build
```

**Important:** Run `airflow-init` **before** scheduler/webserver. If you see `airflow db init` errors, the database was never migrated.

Check health:

```cmd
curl http://localhost:8000/health
curl http://localhost:8001/docs
curl http://localhost:8002/health
```

### 4. Trigger Airflow DAGs (in this order)

In **Airflow UI** → DAGs → Unpause → Trigger (play button), **or** CMD:

```cmd
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow dags trigger supply_chain_ingestion
```

Wait until **green**, then:

```cmd
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow dags trigger supply_chain_transformation
```

Wait until **green**, then:

```cmd
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow dags trigger supply_chain_iceberg_compaction
```

Wait until **green**, then:

```cmd
docker compose -f docker-compose.airflow.yml exec -T airflow-scheduler airflow dags trigger supply_chain_semantic
```

| DAG | What it does |
|-----|----------------|
| `supply_chain_ingestion` | CSV → ingestion API → Bronze |
| `supply_chain_transformation` | Bronze → Silver → **Gold replenishment** |
| `supply_chain_iceberg_compaction` | Iceberg file maintenance |
| `supply_chain_semantic` | Parquet export → dbt → SQL workloads |

**Before `supply_chain_semantic`:** Part 3 must be running (`analytics-service` on port 8002). The DAG includes `wait_for_analytics_service`; if it fails, run:

```cmd
docker compose -f docker-compose.part3.yml up -d --build analytics-service
curl http://localhost:8002/health
```

### 5. Inventory Silver (if `silver.inventory_transactions` was missing)

```cmd
curl -X POST http://localhost:8001/transform/silver/src_inventory_transactions
```

### 6. Refresh Part 3 analytics + BI

```cmd
docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export

curl -X POST http://localhost:8002/analytics/run-all
curl http://localhost:8002/semantic/executive-dashboard
curl http://localhost:8002/semantic/prometheus-bi-catalog
```

### 7. Open dashboards

| URL | Purpose |
|-----|---------|
| http://localhost:3000/d/supply-chain-bi | Grafana executive scorecards |
| http://localhost:8002/dashboard | Top-10 SKUs, trends, tables |
| http://localhost:9090/targets | Prometheus — `analytics-service` must be **UP** |

Wait **30–60 seconds** after step 6, then **refresh Grafana** (Ctrl+F5).

---

## Verify data is correct

```cmd
curl http://localhost:8002/semantic/prometheus-bi-catalog
```

Expect **15+** metric names including:

- `stockout_risk_pct`
- `critical_skus_count`
- `replenishment_value_million_pkr`
- `max_urgency_score`

```cmd
curl http://localhost:8002/metrics/prometheus | findstr stockout_risk_pct
```

Should print a line with a numeric value (not empty).

---

## Common failures

| Symptom | Fix |
|---------|-----|
| **Prometheus stopped / not starting** | Start: `docker compose -f docker-compose.yml up -d prometheus`. Check: http://localhost:9090/targets |
| **Transformation bronze checks `Connection refused`** | Part 1 `ingestion-api` not running. Start: `docker compose -f docker-compose.yml up -d ingestion-api`. Verify: `curl http://localhost:8000/health`. Re-run DAG from `wait_for_ingestion_api`. |
| **analytics-service unhealthy** | Startup used to load all Iceberg before `/health` (OOM / timeout). Rebuild Part 3 after pull. `docker compose -f docker-compose.part3.yml rm -sf analytics-service` then `up -d --build analytics-service`. `curl http://localhost:8002/health` must return ok within ~1 min. |
| **Grafana all panels No data** | Part 3 + Part 1 up. Check http://localhost:9090/targets. Grafana datasource: http://pipeline-prometheus:9090. Then `curl -X POST http://localhost:8002/analytics/run-all`, Ctrl+F5 Grafana. |
| **Silver inventory slow / 1M+ Bronze reads** | Incremental CDC is enabled: watermark at `storage/checkpoints/silver_bronze_src_inventory_transactions.json`. After Silver is built once, set `SILVER_STAMP_WATERMARK=1` on transform-service and re-run inventory Silver (skips Bronze scan). Or delete watermark + `SILVER_RESET_WATERMARK=1` for one-time bootstrap. |
| **airflow-init stuck** after `AF_UID=... STORAGE=...` | Cancel (Ctrl+C). Old init ran `chown -R` on all of `storage/` which can hang for hours on Windows. Re-run with latest compose — should reach `running airflow db migrate` in seconds. |
| **`gosu: command not found`** | Fixed: init runs as `airflow` user (`AIRFLOW_UID`), no `gosu`. Re-run `docker compose -f docker-compose.airflow.yml run --rm airflow-init`. |
| **airflow-init logs empty** in Docker Desktop | Init is a one-shot container; logs only appear after it **runs**. Force a fresh run in the foreground: `docker compose -f docker-compose.airflow.yml run --rm airflow-init` (output streams in CMD). Or: `docker compose -f docker-compose.airflow.yml up --force-recreate airflow-init` (no `-d`). After `up -d`, read logs with `docker compose -f docker-compose.airflow.yml logs airflow-init`. If still empty, the container may be stuck in **Created** — wait for `airflow-postgres` healthy, then recreate init. |
| Compaction **Permission denied** on `.session.lock` | Run `scripts\fix-iceberg-storage.cmd`, re-run compaction DAG |
| Grafana **No data** on new KPIs | Rebuild analytics: `docker compose -f docker-compose.part3.yml up -d --build analytics-service`, then step 6 |
| **9108 SKUs**, **91% catalog at risk** | Expected with demo data (most products below threshold); use **top-10 table** on :8002/dashboard for actions |
| Semantic DAG fails on export | Ensure transformation DAG finished; `iceberg-toolkit` image built in Airflow Dockerfile |
| `run-all` / `run_analytics_sql` **RemoteDisconnected** | Analytics OOM: old code loaded Iceberg 8×. Rebuild Part 3: `docker compose -f docker-compose.part3.yml up -d --build analytics-service`. Ensure transformation + semantic export finished. Test: `curl -X POST http://localhost:8002/analytics/run-all` (may take several minutes). |
| `run-all` errors | Run transformation + `semantic-export` first; check `curl http://localhost:8002/analytics/run-all` JSON for `status: ok` |

---

## Keep IoT / CDC running (optional)

For live RFID and inventory CDC, Part 1 stack should include:

- `iot-producer`, `iot-consumer`, `cdc-consumer`, `batch-runner`

They start with `docker compose -f docker-compose.yml up -d`.

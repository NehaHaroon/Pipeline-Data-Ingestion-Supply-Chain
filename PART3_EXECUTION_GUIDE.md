# Part 3 Execution Guide — Lakehouse + Semantic Analytics

## Docker layout (three compose files)

| File | Purpose | Key services |
|------|---------|----------------|
| `docker-compose.yml` | Part 1 — Ingestion | Kafka, ingestion-api, CDC, **prometheus** |
| `docker-compose.airflow.yml` | Part 2 — Transformation | Airflow, **transform-service** (:8001) |
| `docker-compose.part3.yml` | Part 3 — Analytics & BI | **analytics-service** (:8002), **Grafana** (:3000) |

All stacks share the external Docker network **`pipeline-net`** (created by `docker-compose.yml`).

### Start order

```bash
# 1) Ingestion (+ Prometheus for Grafana)
docker compose -f docker-compose.yml up -d --build

# 2) Transformation (Airflow + transform-service)
docker compose -f docker-compose.airflow.yml up -d --build

# 3) Part 3 analytics & BI
docker compose -f docker-compose.part3.yml up -d --build
```

### Part 3 image

Built from **`Dockerfile.part3`** (`requirements-part3.txt`: DuckDB, dbt-core, dbt-duckdb + base pipeline deps).

```bash
docker compose -f docker-compose.part3.yml build analytics-service
```

### Optional one-shot jobs (profiles)

```bash
# Export parquet + run 8 SQL workloads
docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export

# Export + dbt run + dbt test
docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-dbt
```

### Stop

```bash
docker compose -f docker-compose.part3.yml down
docker compose -f docker-compose.airflow.yml down
docker compose -f docker-compose.yml down
```

## `Permission denied: query_audit.jsonl`

On Windows bind mounts, `storage/analytics/query_audit.jsonl` may be owned by another user/container.

**If PowerShell scripts are blocked by group policy**, use any of these instead:

```cmd
REM Option A — CMD batch (no PowerShell)
scripts\fix-iceberg-storage.cmd
```

```cmd
REM Option B — Python (recommended)
python scripts\fix_iceberg_storage.py
```

```cmd
REM Option C — Docker only (no host scripts)
docker compose -f docker-compose.part3.yml run --rm --user 0:0 --entrypoint bash analytics-service -c "mkdir -p /app/storage/analytics && chmod -R a+rwX /app/storage/analytics"
```

```cmd
REM Option D — delete files manually in File Explorer
REM   storage\analytics\query_audit.jsonl
REM   storage\analytics\metrics_state.json
REM   storage\.locks\  (folder)
```

Then:

```cmd
docker compose -f docker-compose.part3.yml up -d --build
```

Analytics falls back to `/tmp/query_audit.jsonl` inside the container if the bind mount is still not writable (SQL workloads still complete).

---

## `silver.inventory_transactions` = 0 / table does not exist

Silver was not built for CDC inventory. After Bronze has data:

1. Re-run **`supply_chain_transformation`** (includes `src_inventory_transactions` in `SOURCES_FOR_SILVER`), or  
2. `curl -X POST http://localhost:8001/transform/silver/src_inventory_transactions`  
3. Re-run `semantic-export`.

`run_part3_pipeline.py` auto-calls transform-service when export count is 0.

---

## Grafana panels show "No data" for new KPIs (% catalog, M PKR, etc.)

**Cause:** Prometheus was reading an old `metrics_state.json` on disk (or an empty file when the bind mount is not writable). Only legacy metrics (`skus_needing_replenishment`, `avg_urgency_score`) appeared.

**Fix (code):** `/metrics/prometheus` now exports **in-memory** BI KPIs after each refresh.

**Verify:**

```cmd
docker compose -f docker-compose.part3.yml up -d --build analytics-service
curl http://localhost:8002/semantic/prometheus-bi-catalog
curl http://localhost:8002/metrics/prometheus | findstr stockout_risk
```

You should see `stockout_risk_pct`, `critical_skus_count`, `replenishment_value_million_pkr`, `max_urgency_score`, etc.

In Prometheus UI (http://localhost:9090) → Status → Targets → `analytics-service` should be **UP**. Wait 1–2 scrape intervals, then refresh Grafana.

---

## Grafana KPIs show 0 / `run-all` returns `_tmp_*` table errors

**Cause (fixed in code):** DuckDB views pointed at unregistered `_tmp_*` relations. Rebuild analytics-service after pulling.

**You still need data in Iceberg:**

1. Run ingestion + **`supply_chain_transformation`** (Silver must include `src_inventory_transactions`).
2. Confirm Gold: `gold.replenishment_signals` has rows (warehouse + IoT Silver non-empty).
3. Optional parquet fallback: `docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export`
4. Re-run workloads:

```powershell
curl -X POST http://localhost:8002/analytics/run-all
curl http://localhost:8002/semantic/bi-kpis
```

5. Grafana refreshes from Prometheus after step 4 (scrape interval ~15–30s).

`adhoc_001` may show **blocked** during business hours (cost policy) — retry off-peak or use `POST /analytics/query/adhoc_001` after hours.

---

## Compaction / Iceberg lock errors (Permission denied)

If `supply_chain_iceberg_compaction` fails on `iceberg_catalog.db.session.lock` or `storage/.locks/`:

1. Stop heavy writers (optional): pause transformation/compaction DAGs.
2. From repo root (PowerShell):

```cmd
python scripts\fix_iceberg_storage.py --docker-chown
```

3. Ensure `.env` has the same `AIRFLOW_UID` and `PIPELINE_UID` (default `50000`).
4. Recreate Airflow init + stack:

```powershell
docker compose -f docker-compose.airflow.yml run --rm airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
```

5. Retry the compaction DAG.

Lock files now live under `storage/.locks/iceberg_catalog.session.lock` (world-writable dir) so ingestion-api and Airflow share one catalog safely.

---

## Architecture

```
Sources → CDC/Stream → Iceberg Bronze/Silver/Gold → dbt Semantic Layer → BI (Grafana) → Workload Monitoring
         ↑ Control Plane (governance + contracts)     ↑ Observability Plane
```

Ensure Parts 1–2 have populated Iceberg tables (run ingestion + transformation DAGs or `test_transformation.py`) before Part 3 SQL/dbt.

**Airflow `supply_chain_semantic` DAG:** `export_semantic_parquet` uses the **iceberg-toolkit** venv (not Airflow’s SQLAlchemy 1.4). `dbt_*` and `run_analytics_sql` require **analytics-service** (`docker-compose.part3.yml` up).

## Step-by-step (project tasks)

| Task | Action | Verify |
|------|--------|--------|
| 1 Governance | `GET http://localhost:8002/governance/policies` | 12 policies |
| 2 Contracts | `GET http://localhost:8002/contracts/enterprise` | structural + semantic + freshness + lineage |
| 3 SQL workloads | `POST http://localhost:8002/analytics/run-all` | 8 queries |
| 4 BI design | `docs/part3/TASK4_BI_DASHBOARD_DESIGN.md` | Report wireframe |
| 5 dbt | `docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-dbt` | tests pass |
| 6 BI dashboard | **Grafana** http://localhost:3000 → **Supply Chain Executive BI**; **REST UI** http://localhost:8002/dashboard | Live panels + KPI cards from `/semantic/bi-kpis` |
| 7 Workload monitoring | **Analytical Workload Monitoring** dashboard | KPI categories |
| 8 Video | 5–8 min demo across all three compose stacks | — |

## Local install (does not break Parts 1–2)

`requirements.txt` is **unchanged** for ingestion/transformation. Part 3 adds only `requirements-part3-extras.txt` (dbt).

### Safe default (recommended)

Use this if ingestion/transformation already run on your machine:

```powershell
pip install -r requirements-part3-extras.txt
# or
powershell -ExecutionPolicy Bypass -File scripts/install-part3.ps1
```

This installs **only dbt** and does not reinstall or downgrade `pyarrow` / `pyiceberg`.

### Full Part 3 venv (new machine only)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install-part3.ps1 -Full
```

Uses `constraints-part3.txt` so pip does not downgrade PyArrow. Prefer **Python 3.11** (not 3.14).

### Docker (isolated image, zero impact on local pip)

```powershell
docker compose -f docker-compose.part3.yml build analytics-service
```

Uses `Dockerfile.part3` (Python 3.11) — does not change `Dockerfile` for ingestion.

## Local run (without Docker)

```bash
python scripts/run_part3_pipeline.py
```

## API endpoints (analytics-service :8002)

- `GET /governance/policies`
- `GET /contracts/enterprise`
- `GET /analytics/catalog`
- `POST /analytics/query/{query_id}`
- `POST /analytics/run-all`
- `POST /semantic/dbt/run`
- `GET /metrics/prometheus` — scraped by Prometheus from ingestion stack

## Access URLs

| Service | URL |
|---------|-----|
| Ingestion API | http://localhost:8000 |
| Transform API | http://localhost:8001 |
| Analytics API | http://localhost:8002 |
| Airflow | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

## Task 8 video script

1. `docker compose -f docker-compose.yml ps` then airflow then part3
2. Airflow ingestion + transformation DAGs green
3. `docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-export`
4. Grafana BI + workload dashboards refresh
5. `curl http://localhost:8002/analytics/catalog`

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

## Compaction / Iceberg lock errors (Permission denied)

If `supply_chain_iceberg_compaction` fails on `iceberg_catalog.db.session.lock` or `storage/.locks/`:

1. Stop heavy writers (optional): pause transformation/compaction DAGs.
2. From repo root (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/fix-iceberg-storage.ps1 -DockerChown
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

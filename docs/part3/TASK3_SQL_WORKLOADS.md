# Task 3 — Analytical SQL Workloads

Eight queries (two per workload type). Execute via:

```bash
POST http://localhost:8002/analytics/run-all
# or individually: POST /analytics/query/op_001
```

## Catalog summary

| ID | Workload | Name | Complexity | Key characteristics |
|----|----------|------|------------|---------------------|
| op_001 | Operational | Current shelf inventory | low | partition_pruning, latest_by, no join |
| op_002 | Operational | Delayed CDC movements | medium | time_filter, late_arriving |
| str_001 | Strategic | Monthly revenue trends | analytical | large aggregation, time bucket |
| str_002 | Strategic | Warehouse performance | resource_intensive | multi-join, group by warehouse |
| exec_001 | Executive BI | KPI scorecard | medium | star schema, scalar aggregates |
| exec_002 | Executive BI | Profitability (PKR) | analytical | joins, derived metrics |
| adhoc_001 | Ad hoc | Exploratory cross-source join | resource_intensive | investigative multi-join |
| adhoc_002 | Ad hoc | Defect root cause | analytical | joins, filters |

## Optimization plans (per query)

See `data_plane/analytics/query_catalog.py` or `GET /analytics/catalog` for full `optimization_plan` arrays covering:

- **Partitioning** — product_id, warehouse_location, timestamp filters
- **Compaction** — weekly on high-churn Silver tables
- **Materialized views** — dbt marts (`fct_daily_revenue`, `dim_warehouse`)
- **Iceberg snapshots** — time travel for strategic YoY compares
- **Caching** — analytics-service TTL cache for op_001; query hash cache for ad hoc

## SQL files

Located under `data_plane/analytics/sql/{operational,strategic,executive_bi,ad_hoc}/`.

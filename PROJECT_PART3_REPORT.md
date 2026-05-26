# Data Engineering Project — Part 3  
## Supply Chain Lakehouse Analytics Platform

**Author:** Neha Haroon  
**Programme:** MSc Computer Science  
**Institution:** [Your University]  
**Date:** May 2026  
**Repository:** Pipeline-Data-Ingestion-Supply-Chain  

---

## Abstract

Parts 1 and 2 of this project established a production-oriented ingestion and transformation pipeline for a multi-source supply chain domain. Part 3 extends that foundation into a lakehouse analytics platform: governed data contracts, workload-classified SQL over Iceberg medallion tables, a dbt semantic layer, executive BI in Grafana, and a dedicated analytical workload observability stack. The design follows a four-plane architecture (Control, Data, Observability, Semantic) aligned with modern practice at Databricks- and Snowflake-style platforms, while remaining deployable on Docker Compose with Apache Iceberg, Airflow, and DuckDB.

The business problem addressed is **inventory replenishment under uncertainty**: operations teams need near-real-time shelf visibility, planners need regional demand history, and executives need PKR-denominated risk exposure when weather or supplier delays degrade service levels. This report documents requirements traceability, implementation decisions, and evidence paths for assessment.

---

## 1. Introduction and scope

### 1.1 Motivation

Batch CSV sources, PostgreSQL CDC, and Kafka IoT streams (defined in `control_plane/entities.py`) already flow through Bronze, Silver, and Gold Iceberg tables. Part 3 adds the **consumption layer** that turns curated data into governed analytics: without semantic models and workload policies, each dashboard would re-implement joins and business rules, and query cost would be uncontrolled.

### 1.2 Objectives (from `project.part3.md`)

1. Data governance policies with operational meaning.  
2. Enterprise-grade contracts (structural, semantic, freshness, lineage).  
3. Eight SQL workloads across operational, strategic, executive BI, and ad hoc classes.  
4. BI dashboard design and implementation tied to semantic metrics.  
5. dbt semantic layer (staging → marts) with tests and lineage.  
6. Workload monitoring dashboard with KPIs across five categories.  
7. End-to-end automation and demonstration (video, 5–8 minutes — deliverable separate from this repository).

### 1.3 Architectural placement

```
Sources (7) → Ingestion API / CDC / Stream → Iceberg Bronze
         → SilverTransformer → Iceberg Silver
         → GoldAggregator → gold.replenishment_signals
         → export_for_dbt (parquet) → dbt marts
         → analytics-service (DuckDB SQL) → Grafana + Prometheus
```

Deployment is split across three Compose files so Part 3 does not destabilise Parts 1–2:

| Compose file | Responsibility |
|--------------|----------------|
| `docker-compose.yml` | Kafka, ingestion-api, Postgres CDC, Prometheus |
| `docker-compose.airflow.yml` | Airflow, transform-service (:8001) |
| `docker-compose.part3.yml` | analytics-service (:8002), Grafana |

---

## 2. Control plane — Task 1: Data governance (6%)

Implementation: `control_plane/governance_policies.py` (12 policies), narrative summary in `docs/part3/TASK1_GOVERNANCE.md`, API exposure at `GET /governance/policies` on port 8002.

### 2.1 Policy catalogue

Two policies per category were defined with **business value**, **operational impact**, and **tradeoffs**, as required.

| Category | Policy IDs | Essence |
|----------|------------|---------|
| Data access | gov_access_001, gov_access_002 | Role-based access; 24h Bronze window for ops streaming |
| Retention | gov_retention_001, gov_retention_002 | Bronze 30d / Silver 365d / Gold indefinite; telemetry 180d |
| Cost governance | gov_cost_001, gov_cost_002 | Business-hours join limits; 120s query wall time |
| Workload isolation | gov_isolation_001, gov_isolation_002 | Docker service pools; dbt after Gold |
| Schema evolution | gov_schema_001, gov_schema_002 | Additive Iceberg; dbt tests gate BI |
| Data quality SLA | gov_sla_001, gov_sla_002 | Null &lt;2%; freshness &lt;5 min; exec p95 &lt;3s |

### 2.2 Enforcement (not only documentation)

- **Cost:** `data_plane/analytics/query_executor.py` blocks ad hoc joins &gt;3 during business hours (PKT-aligned UTC window).  
- **Access:** `control_plane/access_control.py` enforces `X-Analytics-Role` on `POST /analytics/query/{id}` (finance → executive queries only).  
- **Freshness:** Silver `late_arriving` flag uses per-source `max_lag_seconds` from enterprise freshness contracts (120s for IoT/CDC).  

We deliberately did not implement full IAM integration; roles are header-based for reproducibility in the academic environment, with a clear migration path to OAuth2 scopes in production.

---

## 3. Control plane — Task 2: Advanced data contracts (4%)

Part 1 contracts in `control_plane/contracts.py` enforce **ingestion-boundary** rules (seven sources, field constraints, violation policies). Part 3 **enterprise contracts** extend this to the lakehouse path in `control_plane/enterprise_contract_builder.py`, registered via `control_plane/advanced_contracts.py`.

### 3.1 Coverage

Contracts are generated for **every** `ALL_SOURCES` entry in `entities.py`, plus `gold.replenishment_signals`:

| source_id | Ingestion contract | Enterprise structural | Semantic highlights | Freshness (max lag) |
|-----------|-------------------|----------------------|---------------------|-------------------|
| src_warehouse_master | contract_warehouse_v1 | PK product_id | USD unit_cost, non-negative inventory | 24h |
| src_sales_history | contract_sales_v1 | PK receipt_id | PKR revenue in marts; returns | 24h |
| src_manufacturing_logs | contract_manufacturing_v1 | PK production_batch_id | defect rate bounds | 24h |
| src_legacy_trends | contract_legacy_v1 | PK code + period | regional baseline | 7d |
| src_inventory_transactions | contract_inventory_txn_v1 | PK transaction_id | **shipment_status** ∈ Pending/InTransit/Delivered | 2 min, quarantine |
| src_iot_rfid_stream | contract_iot_v1 | PK event_id | shelf stock ≥0, ZONE enums | 2 min, quarantine |
| src_weather_api | contract_weather_v1 | city + timestamp | weather_risk on Gold | 3 min |
| gold_replenishment_signals | — | PK product_id | PKR, urgency [0,1] | 5 min |

### 3.2 Contract types

- **Structural:** schema version, columns, required fields, primary keys, partition keys (aligned with Silver write patterns in `silver_transformer.py`).  
- **Semantic:** derived from `FieldConstraint` in `contracts.py`; inventory adds shipment_status semantics for logistics KPIs.  
- **Freshness:** `max_lag_seconds` derived from `IngestionFrequency` in `entities.py`; streaming sources quarantine on breach.  
- **Lineage:** edges Source → bronze → silver → gold/marts → `grafana:supply-chain-bi`; catalog via `GET /contracts/enterprise`.

---

## 4. Data plane — Task 3: Analytical SQL workloads (10%)

### 4.1 Workload design

Eight queries live under `data_plane/analytics/sql/`, catalogued in `data_plane/analytics/query_catalog.py`, executed by DuckDB over Iceberg-backed views in `data_plane/analytics/duckdb_engine.py`.

| ID | Class | Name | Complexity | Key characteristics |
|----|-------|------|------------|---------------------|
| op_001 | Operational | Current shelf inventory | low | latest_by product_id, partition pruning |
| op_002 | Operational | Supplier bottlenecks | medium | 24h window, joins IoT + warehouse |
| str_001 | Strategic | Monthly revenue trends | analytical | month bucket, legacy + sales |
| str_002 | Strategic | Warehouse performance | resource intensive | group by warehouse |
| exec_001 | Executive BI | KPI scorecard | medium | aggregates on Gold |
| exec_002 | Executive BI | Profitability (PKR) | analytical | star-style join Gold + warehouse |
| adhoc_001 | Ad hoc | Exploratory cross-source join | resource intensive | multi-join investigation |
| adhoc_002 | Ad hoc | Defect root cause | analytical | mfg vs returns correlation |

Delayed CDC events (&gt;2 min) are implemented in `operational/delayed_shipments.sql` and surfaced through `fct_inventory_movements.is_delayed` and the **delivery_delay_rate** metric — supporting the brief’s “delayed shipments” example without exceeding two operational query slots.

### 4.2 Optimization strategy (summary)

Each query record includes an explicit `optimization_plan` in the catalog: Iceberg partition pruning, weekly compaction (`compaction_dag.py`), dbt materialised marts, snapshot time travel for strategic YoY, and analytics-service caching for op_001. Evidence of execution can be regenerated:

```bash
python scripts/generate_part3_evidence.py
# → storage/analytics/workload_execution_report.json
```

---

## 5. Task 4: BI dashboard design (3%)

The paper prototype is documented in `docs/part3/TASK4_BI_DASHBOARD_DESIGN.md`. It maps the **replenishment command centre** layout: scorecards (SKUs at risk, urgency, PKR exposure, weather), bar chart for top priorities, shelf vs threshold chart, revenue trend, warehouse table.

**Business fit:** The dashboard answers whether we will stock out before the next delivery cycle, and quantifies order value at risk in PKR — directly tied to `gold.replenishment_signals` and executive SQL `exec_001` / `exec_002`.

**For submission:** Export the wireframe section as a figure in your PDF report (screenshot from the markdown structure or redraw in draw.io).

---

## 6. Semantic plane — Task 5: dbt implementation (7%)

Project path: `semantic_plane/dbt/supply_chain_semantic/`

### 6.1 Model layers

| Layer | Models |
|-------|--------|
| Staging | stg_sales_history, stg_warehouse_master, stg_inventory_transactions, stg_iot_rfid_stream |
| Intermediate | int_daily_sales, int_latest_shelf_stock |
| Marts | fct_replenishment_signals, fct_daily_revenue, fct_inventory_movements, dim_warehouse |
| Metrics | metric_daily_order_growth |

Parquet inputs are exported from Iceberg by `semantic_plane/export_for_dbt.py` (invoked from Airflow `export_semantic_parquet` via iceberg-toolkit — avoids SQLAlchemy 1.4 conflict in the Airflow Python process).

### 6.2 Semantic metrics (`semantic_plane/metric_catalog.py`)

| metric_id | dbt anchor |
|-----------|------------|
| total_revenue | fct_daily_revenue (PKR) |
| delivery_delay_rate | fct_inventory_movements |
| supplier_reliability_score | dim_warehouse |
| inventory_turnover | fct_replenishment_signals |
| daily_order_growth | metric_daily_order_growth |

### 6.3 Tests and lineage

- **Tests:** `models/staging/schema.yml`, `models/marts/schema.yml` — uniqueness, not_null, accepted_values, **relationships** to stg_warehouse_master.  
- **Lineage / BI link:** `models/exposures.yml` maps `supply_chain_executive_dashboard` to marts.  
- **Documentation:** run `dbt docs generate && dbt docs serve` after `semantic-dbt` Compose profile or local dbt.

---

## 7. Task 6: BI dashboard implementation (4%)

Built in Grafana: `monitoring/grafana/dashboards/supply-chain-bi.json` (uid: `supply-chain-bi`).

**Semantic connection:** Panels read `analytics_bi_metric` Prometheus series populated when `/metrics/prometheus` is scraped — metrics are computed by `data_plane/analytics/bi_kpis.py`, which executes **exec_001** and **exec_002** against the same DuckDB views as the semantic marts. Thus the dashboard reflects mart logic, not placeholder counters.

| Panel | Metric | Source query / mart |
|-------|--------|---------------------|
| SKUs Needing Replenishment | skus_needing_replenishment | exec_001 |
| Avg Urgency | avg_urgency_score | exec_001 |
| Weather Risk | weather_risk_active | exec_001 |
| Replenishment PKR | replenishment_value_pkr | exec_002 |

Refresh interval: 30s. Region template variable prepared for warehouse filter extension.

---

## 8. Observability plane — Task 7: Workload monitoring (8%)

Dashboard: `monitoring/grafana/dashboards/analytical-workload-monitoring.json`

Metrics collector: `observability_plane/analytics_metrics.py`

| Category | KPIs implemented | Source |
|----------|------------------|--------|
| Query performance | avg/p95 latency, failure rate, concurrent, queue | Query audit log + in-memory deque |
| Resource | CPU/memory/disk proxy, cache hit, **partition_pruning_efficiency**, **snapshot_scan_count** | `storage_plane/storage_kpis.py` |
| BI usage | refresh frequency, load time, active users, expensive dashboard ms | BI KPI refresh |
| SQL workload | bytes_scanned_per_query, top expensive (API `/analytics/metrics`) | Audit JSONL |
| Semantic layer | dbt runtime, failed tests, stale models, lineage depth | dbt API callbacks |

At least two KPIs per category are satisfied. Resource CPU/memory are **derived proxies** from Iceberg small-file ratios and storage MB (honest limitation: no cAdvisor sidecar in the academic stack).

---

## 9. Task 8: Automation and demonstration (4%)

### 9.1 Orchestration

| Step | Mechanism |
|------|-----------|
| Ingestion | `supply_chain_ingestion` DAG / `docker-compose.yml` |
| Transform | `supply_chain_transformation` DAG, transform-service |
| Semantic | `supply_chain_semantic` DAG: export → dbt (analytics-service) → SQL workloads |
| BI refresh | Grafana auto-refresh + `GET /semantic/bi-kpis` |

Airflow semantic export uses `scripts/run_iceberg_task.py export-semantic-parquet` in the **iceberg-toolkit** venv (fixes `DeclarativeBase` / SQLAlchemy 2.x requirement for PyIceberg).

### 9.2 Video checklist (5–8 min)

1. `docker compose` three stacks healthy.  
2. Airflow: ingestion + transformation green.  
3. Trigger `supply_chain_semantic` or `semantic-export` profile.  
4. Open Grafana BI + workload dashboards.  
5. Show `storage/analytics/workload_execution_report.json` or `curl localhost:8002/analytics/catalog`.

---

## 10. Four-plane summary

| Plane | Components |
|-------|------------|
| Control | entities.py, contracts.py, governance_policies.py, advanced_contracts.py, access_control.py |
| Data | ingestion, CDC, silver/gold, analytics-service, 8 SQL workloads |
| Observability | telemetry (P1), transformation_kpis, analytics_metrics, Prometheus, Grafana |
| Semantic | dbt project, metric_catalog, exposures, export_for_dbt |

---

## 11. Evaluation and limitations

**Strengths**

- End-to-end traceability from `entities.py` / `contracts.py` through enterprise contracts and dbt marts.  
- Workload isolation via separate Docker images and Airflow toolkit pattern.  
- BI panels tied to executive SQL, not arbitrary Prometheus placeholders.

**Limitations (stated for academic integrity)**

- RBAC is header-simulated, not enterprise IdP.  
- Resource metrics approximate host utilisation from storage heuristics.  
- dbt docs artefacts are generated at runtime, not committed.  
- Video proof is external to the repo.

---

## 12. Conclusion

Part 3 completes the transition from a ingestion/transformation pipeline to a **governed lakehouse analytics platform** for supply chain replenishment. All eight tasks from `project.part3.md` are addressed in code, configuration, or documented deliverables; the remaining submission steps are executing the stack, capturing screenshots, recording the demonstration video, and attaching this report.

---

## Appendix A — Key API endpoints (analytics-service :8002)

| Method | Path |
|--------|------|
| GET | /governance/policies |
| GET | /contracts/enterprise |
| GET | /analytics/catalog |
| POST | /analytics/query/{query_id} |
| POST | /analytics/run-all |
| GET | /semantic/bi-kpis |
| POST | /semantic/dbt/run |
| GET | /metrics/prometheus |

## Appendix B — File index (Part 3)

| Path | Purpose |
|------|---------|
| PROJECT_PART3_REPORT.md | This document |
| PART3_EXECUTION_GUIDE.md | Operator runbook |
| control_plane/governance_policies.py | Task 1 |
| control_plane/enterprise_contract_builder.py | Task 2 (all sources) |
| data_plane/analytics/ | Task 3 |
| semantic_plane/dbt/ | Task 5 |
| monitoring/grafana/dashboards/supply-chain-bi.json | Task 6 |
| monitoring/grafana/dashboards/analytical-workload-monitoring.json | Task 7 |
| airflow/dags/semantic_dag.py | Automation |
| docker-compose.part3.yml | Part 3 runtime |

---

*Submitted in partial fulfilment of the Data Engineering Project — Part 3.*

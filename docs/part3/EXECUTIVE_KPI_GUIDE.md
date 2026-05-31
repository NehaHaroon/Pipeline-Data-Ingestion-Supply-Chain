# Executive KPI Guide — What the numbers mean

**Audience:** Strategy, finance, and supply chain leaders (non-technical).  
**Use case:** Supply Chain Inventory Optimization — avoid stockouts while controlling replenishment spend.

---

## Why old panels were wrong for executives

| Old panel | Problem |
|-----------|---------|
| P95 Query Latency | Measures **database speed**, not business performance |
| Semantic Layer Health (dbt runtime) | **Engineering** metric |
| Raw “9108 SKUs” without context | Gold table only stores **at-risk** rows — need **% of catalog** |
| PKR 609,771,424 | Correct sum but unreadable — use **millions PKR** and top-10 table |
| Suggested order units (flat line) | Gauge over time is not a **demand trend** |

Technical metrics remain on **Analytical Workload Monitoring** (`/d/analytical-workload-monitoring`).

---

## Scorecards (Grafana + browser dashboard)

| KPI | What it means | Action |
|-----|----------------|--------|
| **SKUs at stockout risk** | Products with shelf stock below reorder threshold | Size the buying workload |
| **% of catalog at risk** | At-risk SKUs ÷ total catalog (warehouse master) | >30% = systemic gap, escalate procurement |
| **Critical SKUs (urgency ≥ 50%)** | Highest priority lines on 0–1 urgency scale | Approve POs for these first |
| **Order spend (M PKR)** | Estimated PKR to replenish top at-risk lines (exec_002) | Finance budget approval |
| **Units to order** | Sum of suggested order quantities | Warehouse inbound planning |
| **Late deliveries %** | CDC inventory events flagged `late_arriving` | Call logistics / suppliers |
| **Warehouse bottlenecks** | High outbound + low shelf in same lane | Rebalance stock between sites |
| **Average / peak urgency** | How close to empty vs max capacity | Peak near 100% = imminent stockout |
| **Weather disruption** | Storm / extreme cold on latest weather feed | Add safety stock or delay promises |

---

## Charts (browser dashboard :8002/dashboard)

| Chart | Source | Decision |
|-------|--------|----------|
| Top 10 priorities (bar) | exec_002 | Which product IDs to reorder first |
| Monthly demand trend (line) | str_001 | Regional demand vs legacy baseline |
| Warehouse table | str_002 | Which warehouses have adjustment / throughput issues |
| Priority SKU table | exec_002 | PO line details for buyers |

---

## Data refresh

1. Ingestion + transformation DAGs populate Iceberg Silver/Gold  
2. `GET /semantic/executive-dashboard` or Prometheus scrape refreshes metrics  
3. Grafana auto-refresh 30s; browser dashboard 30s  

```cmd
curl http://localhost:8002/semantic/executive-dashboard
curl http://localhost:8002/metrics/prometheus
```

---

## Your sample numbers interpreted

| Value | Reading |
|-------|---------|
| SKUs 9108 | All rows in Gold replenishment table are below threshold — check **% catalog** for scale |
| Urgency 0.08 (8%) | On average, shelves are **not** critically empty — but tail risk may exist (see peak urgency) |
| Weather 0 | No weather disruption flag on latest feed |
| PKR ~610M | Total replenishment $ for top 50 at-risk lines — use **M PKR** panel and top-10 chart to prioritize |

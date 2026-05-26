# Task 4 — BI Dashboard Paper Prototype (Supply Chain Replenishment)

## Business problem

Warehouse operations lose revenue when shelf stock falls below reorder thresholds while demand remains high. The dashboard answers: **what to reorder, how urgent, and whether weather or supplier issues amplify risk.**

## Layout (wireframe)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  SUPPLY CHAIN COMMAND CENTER          [Region ▼] [Warehouse ▼] [Refresh]   │
├──────────────┬──────────────┬──────────────┬──────────────┬───────────────┤
│ SKUs at Risk │ Avg Urgency  │ Order Value  │ Weather Risk │ Delay Rate    │
│   (scorecard)│  (scorecard) │  PKR (card)  │  (card)      │  (scorecard)  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Top 10 Replenishment Priorities (bar chart — urgency_score)                │
│  SQL: exec_002 profitability_dashboard / mart fct_replenishment_signals     │
├──────────────────────────────┬──────────────────────────────────────────────┤
│  Shelf Stock vs Threshold    │  Monthly Revenue Trend (line)                │
│  SQL: op_001 current_inventory│  SQL: str_001 monthly_revenue_trends        │
├──────────────────────────────┴──────────────────────────────────────────────┤
│  Warehouse Performance (table) — SQL: str_002 supplier_performance          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Interactivity

- **Filters:** `region`, `warehouse_location`, `needs_replenishment=true`
- **Drill-down:** Click SKU → ad-hoc defect_root_cause (adhoc_002)
- **Scorecards:** KPI row refreshes every 30s from semantic layer cache
- **Spacing:** 16px grid; scorecards row height 120px; charts min 300px

## SQL mapping (for Task 5–6)

| UI element | Workload | Query / Model |
|------------|----------|---------------|
| SKUs at Risk | Executive BI | `exec_001` / `total_revenue` metric |
| Delay Rate | Operational | `op_002` / `delivery_delay_rate` |
| Revenue trend | Strategic | `str_001` / `fct_daily_revenue` |
| Replenishment bar | Executive BI | `exec_002` / `fct_replenishment_signals` |

## Implementation

Built in Grafana: **Supply Chain Executive BI** (`uid: supply-chain-bi`) connected to Prometheus metrics fed by `analytics-service` after dbt run.

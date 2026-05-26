# Task 5 — dbt Semantic Layer

## Project location

`semantic_plane/dbt/supply_chain_semantic/`

## Model layers

| Layer | Models | Purpose |
|-------|--------|---------|
| staging | `stg_sales_history`, `stg_warehouse_master`, `stg_inventory_transactions`, `stg_iot_rfid_stream` | Typed views over Silver parquet |
| intermediate | `int_daily_sales`, `int_latest_shelf_stock` | Reusable business logic |
| marts | `fct_replenishment_signals`, `fct_daily_revenue`, `fct_inventory_movements`, `dim_warehouse` | BI-ready star elements |

## Semantic metrics (`semantic_plane/metric_catalog.py`)

- `total_revenue` — PKR revenue from `fct_daily_revenue`
- `delivery_delay_rate` — late CDC ratio
- `supplier_reliability_score` — `dim_warehouse`
- `inventory_turnover` — `fct_replenishment_signals`
- `daily_order_growth` — day-over-day from `fct_daily_revenue`

## Tests

Defined in `models/staging/schema.yml` and `models/marts/schema.yml`:

- uniqueness, not_null, accepted_values, referential integrity (via relationships in marts joins)

## Documentation & lineage

```bash
cd semantic_plane/dbt/supply_chain_semantic
dbt docs generate
dbt docs serve
```

`models/exposures.yml` links Grafana dashboards to mart models for lineage graphs.

"""
Semantic metrics catalog (Part 3 Task 5) — maps BI dashboard elements to business definitions.
"""

METRIC_CATALOG = [
    {
        "metric_id": "total_revenue",
        "label": "Total Revenue (PKR)",
        "definition": "SUM(units_sold * unit_cost) * fx_rate over rolling 30d, net of returns",
        "dbt_model": "marts.fct_daily_revenue",
        "grain": "day",
    },
    {
        "metric_id": "delivery_delay_rate",
        "label": "Delivery Delay Rate",
        "definition": "COUNT(late_arriving=true) / COUNT(*) on inventory transactions",
        "dbt_model": "marts.fct_inventory_movements",
        "grain": "hour",
    },
    {
        "metric_id": "supplier_reliability_score",
        "label": "Supplier Reliability Score",
        "definition": "100 - adjustment_rate_pct per warehouse from strategic workload",
        "dbt_model": "marts.dim_warehouse",
        "grain": "warehouse",
    },
    {
        "metric_id": "inventory_turnover",
        "label": "Inventory Turnover",
        "definition": "units_sold_7d / NULLIF(current_stock_on_shelf, 0)",
        "dbt_model": "marts.fct_replenishment_signals",
        "grain": "product",
    },
    {
        "metric_id": "daily_order_growth",
        "label": "Daily Order Growth %",
        "definition": "(orders_today - orders_yesterday) / orders_yesterday",
        "dbt_model": "marts.fct_daily_revenue",
        "grain": "day",
    },
]

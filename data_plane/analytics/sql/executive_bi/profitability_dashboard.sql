-- exec_002: Profitability & efficiency (PKR via fx_rate=278)
WITH fx AS (SELECT 278.0 AS pkr_per_usd)
SELECT
    g.product_id,
    g.urgency_score,
    g.suggested_order_qty,
    w.unit_cost * fx.pkr_per_usd AS unit_cost_pkr,
    g.suggested_order_qty * w.unit_cost * fx.pkr_per_usd AS replenishment_value_pkr,
    g.units_sold_7d,
    g.current_stock_on_shelf,
    g.weather_risk
FROM gold_replenishment_signals g
LEFT JOIN silver_warehouse_master w ON g.product_id = w.product_id
CROSS JOIN fx
WHERE g.needs_replenishment = TRUE
ORDER BY replenishment_value_pkr DESC
LIMIT 50;

-- exec_001: Executive KPI scorecard (star-schema style aggregates on Gold)
SELECT
    COUNT(*) AS skus_needing_replenishment,
    ROUND(AVG(urgency_score), 4) AS avg_urgency_score,
    ROUND(MAX(urgency_score), 4) AS max_urgency_score,
    SUM(suggested_order_qty) AS total_suggested_order_units,
    MAX(CASE WHEN weather_risk THEN 1 ELSE 0 END) AS weather_risk_flag,
    MAX(gold_computed_at) AS last_gold_refresh
FROM gold_replenishment_signals
WHERE needs_replenishment = TRUE;

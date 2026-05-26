-- adhoc_002: Defect spike root cause analysis
SELECT
    m.product_id,
    COUNT(*) AS batch_count,
    SUM(quantity_produced) AS units_produced,
    SUM(COALESCE(defect_count, 0)) AS total_defects,
    ROUND(
        100.0 * SUM(COALESCE(defect_count, 0)) / NULLIF(SUM(quantity_produced), 0),
        2
    ) AS defect_rate_pct,
    SUM(CASE WHEN s.units_sold < 0 THEN 1 ELSE 0 END) AS return_events
FROM silver_manufacturing_logs m
LEFT JOIN silver_sales_history s
    ON m.product_id = s.product_id
GROUP BY m.product_id
HAVING ROUND(
        100.0 * SUM(COALESCE(defect_count, 0)) / NULLIF(SUM(quantity_produced), 0),
        2
    ) > 5.0
ORDER BY 5 DESC
LIMIT 50;

-- adhoc_001: Exploratory investigative join (resource intensive)
SELECT
    w.product_id,
    w.product_name,
    i.current_stock_on_shelf,
    w.reorder_threshold,
    s.units_sold_7d,
    m.defect_rate_7d,
    s.is_return_rate
FROM silver_warehouse_master w
LEFT JOIN (
    SELECT product_id, current_stock_on_shelf
    FROM (
        SELECT product_id, current_stock_on_shelf,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY timestamp DESC) rn
        FROM silver_iot_rfid_stream
    ) x WHERE rn = 1
) i ON w.product_id = i.product_id
LEFT JOIN (
    SELECT product_id, SUM(units_sold) AS units_sold_7d,
           AVG(CASE WHEN units_sold < 0 THEN 1.0 ELSE 0.0 END) AS is_return_rate
    FROM silver_sales_history
    GROUP BY product_id
) s ON w.product_id = s.product_id
LEFT JOIN (
    SELECT product_id,
           AVG(COALESCE(defect_count, 0) / NULLIF(quantity_produced, 0)) AS defect_rate_7d
    FROM silver_manufacturing_logs
    GROUP BY product_id
) m ON w.product_id = m.product_id
WHERE COALESCE(i.current_stock_on_shelf, 0) < w.reorder_threshold
  AND COALESCE(s.is_return_rate, 0) > 0.1
ORDER BY defect_rate_7d DESC NULLS LAST
LIMIT 100;

-- str_002: Warehouse / supplier performance (multi-join, window, resource intensive)
SELECT
    warehouse_location,
    COUNT(*) AS transaction_count,
    SUM(ABS(quantity_change)) AS total_units_moved,
    SUM(CASE WHEN transaction_type = 'OUT' THEN quantity_change ELSE 0 END) AS outbound_units,
    SUM(CASE WHEN transaction_type = 'IN' THEN quantity_change ELSE 0 END) AS inbound_units,
    ROUND(
        100.0 * SUM(CASE WHEN transaction_type = 'ADJUSTMENT' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS adjustment_rate_pct,
    MAX(timestamp) AS last_activity
FROM silver_inventory_transactions
GROUP BY warehouse_location
ORDER BY total_units_moved DESC;

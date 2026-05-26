-- op_003 (supporting): CDC / stream events breaching 2-minute freshness SLA
SELECT
    transaction_id,
    product_id,
    warehouse_location,
    transaction_type,
    quantity_change,
    timestamp AS event_timestamp,
    late_arriving
FROM silver_inventory_transactions
WHERE COALESCE(late_arriving, FALSE) = TRUE
ORDER BY timestamp DESC
LIMIT 200;

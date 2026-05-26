-- op_001: Current shelf inventory (low latency, near real-time)
SELECT
    product_id,
    current_stock_on_shelf,
    shelf_location,
    timestamp AS last_seen_at
FROM (
    SELECT
        product_id,
        current_stock_on_shelf,
        shelf_location,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY timestamp DESC) AS rn
    FROM silver_iot_rfid_stream
) t
WHERE rn = 1
ORDER BY current_stock_on_shelf ASC
LIMIT 100;

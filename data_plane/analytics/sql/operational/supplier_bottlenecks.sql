-- op_002: Supplier / warehouse bottlenecks — high outbound pressure with low shelf stock
SELECT * FROM (
    SELECT
        t.warehouse_location,
        t.product_id,
        SUM(CASE WHEN t.transaction_type = 'OUT' THEN ABS(t.quantity_change) ELSE 0 END) AS outbound_units_24h,
        MAX(i.current_stock_on_shelf) AS current_shelf_stock,
        w.reorder_threshold,
        CASE
            WHEN MAX(i.current_stock_on_shelf) < w.reorder_threshold
                 AND SUM(CASE WHEN t.transaction_type = 'OUT' THEN 1 ELSE 0 END) >= 3
            THEN 'BOTTLENECK'
            ELSE 'OK'
        END AS bottleneck_status
    FROM silver_inventory_transactions t
    LEFT JOIN (
        SELECT product_id, current_stock_on_shelf
        FROM (
            SELECT product_id, current_stock_on_shelf,
                   ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY timestamp DESC) AS rn
            FROM silver_iot_rfid_stream
        ) x WHERE rn = 1
    ) i ON t.product_id = i.product_id
    LEFT JOIN silver_warehouse_master w ON t.product_id = w.product_id
    WHERE t.timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
    GROUP BY t.warehouse_location, t.product_id, w.reorder_threshold
) bottlenecks
WHERE bottleneck_status = 'BOTTLENECK'
ORDER BY outbound_units_24h DESC
LIMIT 100;

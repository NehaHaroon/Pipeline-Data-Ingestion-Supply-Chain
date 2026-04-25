-- Seeds baseline inventory transactions in an idempotent way.

INSERT INTO inventory_transactions (
    transaction_id,
    product_id,
    warehouse_location,
    transaction_type,
    quantity_change,
    timestamp,
    reference_order_id,
    created_by,
    created_at
)
VALUES
    ('TXN-001-2025-001', 'ART-1001-MID-XS', 'WAREHOUSE-LONDON',  'IN',         500, '2025-11-01T08:00:00Z'::timestamptz, 'PO-2025-0001', 'system',          NOW()),
    ('TXN-001-2025-002', 'ART-1001-MID-S',  'WAREHOUSE-LONDON',  'IN',         300, '2025-11-01T08:15:00Z'::timestamptz, 'PO-2025-0001', 'system',          NOW()),
    ('TXN-001-2025-003', 'ART-1001-MID-M',  'WAREHOUSE-LONDON',  'OUT',        -50, '2025-11-01T09:00:00Z'::timestamptz, 'SO-2025-0101', 'warehouse_staff', NOW()),
    ('TXN-001-2025-004', 'ART-1017-OLI-XS', 'WAREHOUSE-DUBAI',   'IN',         800, '2025-11-01T09:30:00Z'::timestamptz, 'PO-2025-0002', 'system',          NOW()),
    ('TXN-001-2025-005', 'ART-1017-OLI-M',  'WAREHOUSE-DUBAI',   'OUT',       -120, '2025-11-01T10:00:00Z'::timestamptz, 'SO-2025-0102', 'warehouse_staff', NOW()),
    ('TXN-001-2025-006', 'ART-1397-OLI-M',  'WAREHOUSE-KARACHI', 'IN',         450, '2025-11-01T10:45:00Z'::timestamptz, 'PO-2025-0003', 'system',          NOW()),
    ('TXN-001-2025-007', 'ART-1253-NAV-S',  'WAREHOUSE-LONDON',  'OUT',        -75, '2025-11-01T11:15:00Z'::timestamptz, 'SO-2025-0103', 'warehouse_staff', NOW()),
    ('TXN-001-2025-008', 'ART-1222-OLI-L',  'WAREHOUSE-DUBAI',   'ADJUSTMENT',  10, '2025-11-01T12:00:00Z'::timestamptz, NULL,           'quality_control', NOW()),
    ('TXN-001-2025-009', 'ART-1439-CLA-XS', 'WAREHOUSE-KARACHI', 'IN',         600, '2025-11-01T13:30:00Z'::timestamptz, 'PO-2025-0004', 'system',          NOW()),
    ('TXN-001-2025-010', 'ART-1064-NAV-XS', 'WAREHOUSE-LONDON',  'OUT',       -200, '2025-11-01T14:00:00Z'::timestamptz, 'SO-2025-0104', 'warehouse_staff', NOW())
ON CONFLICT (transaction_id) DO NOTHING;

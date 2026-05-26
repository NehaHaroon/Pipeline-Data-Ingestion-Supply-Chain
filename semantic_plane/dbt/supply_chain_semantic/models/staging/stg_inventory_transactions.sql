select
    transaction_id,
    product_id,
    warehouse_location,
    transaction_type,
    quantity_change,
    reference_order_id,
    cast(timestamp as timestamp) as event_timestamp,
    coalesce(late_arriving, false) as late_arriving
from {{ read_parquet('silver_inventory_transactions') }}

select
    product_id,
    product_name,
    reorder_threshold,
    max_capacity,
    unit_cost
from {{ read_parquet('silver_warehouse_master') }}

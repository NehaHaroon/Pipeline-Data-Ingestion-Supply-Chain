select
    receipt_id,
    product_id,
    cast(sale_timestamp as timestamp) as sale_timestamp,
    units_sold,
    store_id,
    coalesce(units_sold < 0, false) as is_return
from {{ read_parquet('silver_sales_history') }}

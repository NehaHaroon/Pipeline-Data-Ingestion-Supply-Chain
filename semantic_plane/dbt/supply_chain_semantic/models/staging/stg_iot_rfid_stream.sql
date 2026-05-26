select
    event_id,
    product_id,
    current_stock_on_shelf,
    shelf_location,
    cast(timestamp as timestamp) as event_timestamp
from {{ read_parquet('silver_iot_rfid_stream') }}

select
    product_id,
    current_stock_on_shelf,
    shelf_location,
    event_timestamp
from (
    select *,
        row_number() over (partition by product_id order by event_timestamp desc) as rn
    from {{ ref('stg_iot_rfid_stream') }}
) t
where rn = 1

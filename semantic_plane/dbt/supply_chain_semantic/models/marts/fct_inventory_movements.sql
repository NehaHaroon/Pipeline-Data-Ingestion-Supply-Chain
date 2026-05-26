select
    transaction_id,
    warehouse_location,
    product_id,
    transaction_type,
    quantity_change,
    event_timestamp,
    late_arriving,
    case
        when transaction_type = 'IN' then 'Pending'
        when transaction_type = 'OUT' and reference_order_id is not null then 'Delivered'
        when transaction_type = 'OUT' then 'InTransit'
        else 'Pending'
    end as shipment_status,
    case when late_arriving then 1 else 0 end as is_delayed
from {{ ref('stg_inventory_transactions') }}

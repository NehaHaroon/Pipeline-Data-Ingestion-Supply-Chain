select
    date_trunc('day', sale_timestamp) as sale_date,
    product_id,
    sum(units_sold) as units_sold,
    sum(case when is_return then abs(units_sold) else 0 end) as return_units
from {{ ref('stg_sales_history') }}
where sale_timestamp is not null
group by 1, 2

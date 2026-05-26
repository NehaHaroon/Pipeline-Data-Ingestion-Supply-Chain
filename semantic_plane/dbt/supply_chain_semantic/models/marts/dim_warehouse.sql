select
    warehouse_location,
    count(*) as transaction_count,
    sum(abs(quantity_change)) as total_units_moved,
    round(
        100.0 * sum(case when transaction_type = 'ADJUSTMENT' then 1 else 0 end)
        / nullif(count(*), 0),
        2
    ) as adjustment_rate_pct,
    greatest(0, 100 - round(
        100.0 * sum(case when transaction_type = 'ADJUSTMENT' then 1 else 0 end)
        / nullif(count(*), 0),
        2
    )) as supplier_reliability_score
from {{ ref('stg_inventory_transactions') }}
group by warehouse_location

with gold as (
    select * from {{ read_parquet('gold_replenishment_signals') }}
),
warehouse as (
    select * from {{ ref('stg_warehouse_master') }}
)
select
    g.product_id,
    g.needs_replenishment,
    g.urgency_score,
    g.suggested_order_qty,
    g.units_sold_7d,
    g.current_stock_on_shelf,
    g.weather_risk,
    w.unit_cost * {{ var('fx_pkr_per_usd') }} as unit_cost_pkr,
    case
        when coalesce(g.current_stock_on_shelf, 0) = 0 then null
        else g.units_sold_7d / g.current_stock_on_shelf
    end as inventory_turnover
from gold g
left join warehouse w on g.product_id = w.product_id
where g.needs_replenishment = true

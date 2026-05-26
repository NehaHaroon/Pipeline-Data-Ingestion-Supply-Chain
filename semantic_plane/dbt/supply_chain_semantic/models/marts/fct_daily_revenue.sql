select
    d.sale_date,
    d.product_id,
    d.units_sold,
    d.return_units,
    w.unit_cost * {{ var('fx_pkr_per_usd') }} as unit_cost_pkr,
    (d.units_sold - d.return_units) * w.unit_cost * {{ var('fx_pkr_per_usd') }} as total_revenue_pkr
from {{ ref('int_daily_sales') }} d
left join {{ ref('stg_warehouse_master') }} w on d.product_id = w.product_id

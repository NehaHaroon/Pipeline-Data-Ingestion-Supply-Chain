-- Semantic metric: daily_order_growth (% change in order volume day-over-day)
with daily as (
    select
        sale_date,
        sum(units_sold) as units_sold
    from {{ ref('fct_daily_revenue') }}
    group by sale_date
),
lagged as (
    select
        sale_date,
        units_sold,
        lag(units_sold) over (order by sale_date) as units_prev_day
    from daily
)
select
    sale_date,
    units_sold,
    units_prev_day,
    case
        when units_prev_day is null or units_prev_day = 0 then null
        else round(100.0 * (units_sold - units_prev_day) / units_prev_day, 2)
    end as daily_order_growth_pct
from lagged
order by sale_date desc

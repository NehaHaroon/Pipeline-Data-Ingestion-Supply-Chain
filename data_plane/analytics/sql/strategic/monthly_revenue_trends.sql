-- str_001: Monthly revenue / units trend (large aggregation, historical)
WITH sales_monthly AS (
    SELECT
        DATE_TRUNC('month', CAST(sale_timestamp AS TIMESTAMP)) AS month,
        COALESCE(store_id, 'UNKNOWN') AS region,
        SUM(units_sold) AS units_sold,
        SUM(CASE WHEN units_sold > 0 THEN units_sold ELSE 0 END) AS gross_units
    FROM silver_sales_history
    WHERE sale_timestamp IS NOT NULL
    GROUP BY 1, 2
),
legacy_monthly AS (
    SELECT
        CAST(historical_period AS VARCHAR) AS month,
        COALESCE(market_region, 'UNKNOWN') AS region,
        SUM(total_monthly_sales) AS legacy_units
    FROM silver_legacy_trends
    GROUP BY 1, 2
)
SELECT
    COALESCE(s.month, l.month) AS month,
    COALESCE(s.region, l.region) AS region,
    COALESCE(s.units_sold, 0) AS current_units,
    COALESCE(l.legacy_units, 0) AS legacy_baseline_units
FROM sales_monthly s
FULL OUTER JOIN legacy_monthly l
    ON s.month = l.month AND s.region = l.region
ORDER BY month DESC, region
LIMIT 500;

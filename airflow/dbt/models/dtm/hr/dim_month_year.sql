{{ config(materialized='table') }}


SELECT
    date,
    'M' as freq_code,
    cast(year as text),
    month_name,
    year || '-' || month_name as month_year
FROM {{ source('dwh', 'time_series') }}
WHERE is_month_end = TRUE
    AND date BETWEEN '2005-01-01' AND CURRENT_DATE
UNION ALL
SELECT
    date,
    'Y' as freq_code,
    cast(year as text),
    month_name,
    year || '-' || month_name as month_year
FROM {{ source('dwh', 'time_series') }}
WHERE is_month_end = TRUE
    AND month = 12
    AND date BETWEEN '2005-01-01' AND CURRENT_DATE
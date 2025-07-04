{{ config(materialized='table') }}


SELECT
    date,
    year || '-' || 'W' || EXTRACT(week from date)  AS week_year,
    year,
    month_name,
    year || '-' || month_name as month_year,
    is_month_end
FROM {{ source('dwh', 'time_series') }}
WHERE date BETWEEN '2005-01-01' AND CURRENT_DATE
AND EXTRACT(DOW FROM date) = 0

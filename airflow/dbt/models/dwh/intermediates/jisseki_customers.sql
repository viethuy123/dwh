{{ config(materialized='table') }}

-- có customer , category
SELECT
    id,
    "companyName" as company_name,
    "name",
    code,
    summary,
    "sale_pic" as sale_name,
    size,
    status,
    type,
    email as email_sale,
    country_id,
    category,
    start_date,
    "created_at" as create_time,
    "updated_at" as update_time,
    etl_datetime
FROM {{ source('jisseki', 'stg_jisseki_customers') }}

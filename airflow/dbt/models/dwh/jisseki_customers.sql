{{ config(materialized='table') }}


SELECT
    a.id as customer_id,
    a."companyName" as company_name,
    a.name as customer_name,
    a.code as customer_code,
    a.summary,
    a.sale_pic as sale_person,
    a.status,
    a.size,
    a.type,
    a.country_id,
    a.active,
    a.created_at,
    a.updated_at,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jisseki', 'stg_customers') }} a



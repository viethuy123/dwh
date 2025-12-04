{{ config(materialized='table') }}


SELECT
    a.id as id,
    a.project_id as project_id,
    a.customer_id as customer_id,
    a.created_at,
    a.updated_at,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jisseki', 'stg_project_customer') }} a



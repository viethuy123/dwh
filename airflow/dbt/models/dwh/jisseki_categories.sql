{{ config(materialized='table') }}


SELECT
    a.id as category_id,
    a.name as category_name,
    a.active as active,
    a.created_at,
    a.updated_at,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jisseki', 'stg_categories') }} a



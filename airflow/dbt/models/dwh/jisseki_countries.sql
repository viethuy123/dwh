{{ config(materialized='table') }}


SELECT
    a.id as country_id,
    a.name as country_name,
    a.code as country_code,
    a.region as region,
    a."subRegion" as sub_region,
    a.language,
    a.created_at,
    a.updated_at,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jisseki', 'stg_countries') }} a



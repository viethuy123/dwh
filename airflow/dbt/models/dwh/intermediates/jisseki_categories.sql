{{ config(materialized='table') }}

-- có customer , category
SELECT
    id,
    name,
    active,
    "name" as category_name,
    "created_at" as create_time,
    "updated_at" as update_time,
    etl_datetime
FROM {{ source('jisseki', 'stg_jisseki_categories') }}

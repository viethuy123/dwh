{{ config(materialized='table') }}

-- có customer , category
SELECT
    id,
    project_id,
    category_id,
    "created_at" as create_time,
    "updated_at" as update_time,
    etl_datetime
FROM {{ source('jisseki', 'stg_jisseki_project_categories') }}

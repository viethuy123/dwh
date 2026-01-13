{{ config(materialized='table') }}

SELECT
    _id as id,
    "projectCategoryName" as project_category_name,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_project_categories') }}
{{ config(materialized='table') }}


SELECT
    _id as id,
    "projectCategoryName" as category_name,
    "projectCategoryDescription" as category_description,
    status,
    "isDeleted" as is_deleted,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    etl_datetime
FROM {{ source('create', 'stg_create_project_categories') }}

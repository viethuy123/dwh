{{ config(materialized='table') }}

SELECT
    a."projectObjId" as project_id,
    a."userObjId" as user_id,
    {{ safe_parse_timestamp('"joinedAt"') }} as joined_at,
    {{ safe_parse_timestamp('"leftAt"') }} as left_at,
    status as status,
    a."isDeleted" as is_deleted,
    a.level as user_level,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_project_members') }} a
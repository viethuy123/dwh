{{ config(materialized='table') }}

SELECT
    a."projectObjId" as project_id,
    a."userObjId" as user_id,
    TO_TIMESTAMP(nullif(a."joinedAt",''),'YYYY-MM-DD HH24:MI:SS') as joined_at,
    TO_TIMESTAMP(nullif(a."leftAt",''),'YYYY-MM-DD HH24:MI:SS') as left_at,
    status as status,
    a."isDeleted" as is_deleted,
    a.level as user_level,
    TO_TIMESTAMP(a."updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_project_members') }} a
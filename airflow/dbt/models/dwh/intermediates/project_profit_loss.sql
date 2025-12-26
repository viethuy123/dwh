{{ config(materialized='table') }}


SELECT
    "projectObjId" as project_id,
    "userObjId" as user_id,
    "departmentObjId" as department_id,
    "branchObjId" as branch_id,
    value as value,
    value1 as value1,
    status as status,
    "isDeleted" as is_deleted,
    "monthAt" as month_at,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_profit_loss_project_expenses') }}

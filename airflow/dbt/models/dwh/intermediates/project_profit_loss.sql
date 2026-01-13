{{ config(materialized='table') }}


SELECT
    _id as id,
    "projectObjId" as project_id,
    "userObjId" as user_id,
    "departmentObjId" as department_id,
    "branchObjId" as branch_id,
    value as value,
    value1 as value1,
    status as status,
    "configCode" as config_code,
    "configObjId" as config_id,
    "isDeleted" as is_deleted,
    "monthAt" as month_at,
    "closingMonthObjId" as closing_month_id,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_profit_loss_project_expenses') }}

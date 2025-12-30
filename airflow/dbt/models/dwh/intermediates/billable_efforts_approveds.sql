{{ config(materialized='table') }}

SELECT
    _id as effort_id,
    "employeeObjId" as user_id,
    "pODObjId" as pod_id,
    "departmentObjId" as department_id,
    effort,
    year || '-' || LPAD(SUBSTRING(month FROM 2), 2, '0') AS month_year,
    role as user_role,
    status,
    "isDeleted" as is_deleted,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_billable_efforts_approveds') }}
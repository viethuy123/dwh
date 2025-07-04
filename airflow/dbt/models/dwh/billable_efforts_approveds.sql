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
    TO_TIMESTAMP("createdAt",'YYYY-MM-DD HH24:MI:SS') as created_time,
    TO_TIMESTAMP("updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_billable_efforts_approveds') }}
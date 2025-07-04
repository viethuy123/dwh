{{ config(materialized='table') }}


SELECT
    _id as department_id,
    "branchObjId" as branch_id,
    "departmentName" as department_name,
    level,
    children,
    "parentObjId" as parent_id,
    status,
    "isDeleted" as is_deleted,
    TO_TIMESTAMP("updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_company_departments') }}

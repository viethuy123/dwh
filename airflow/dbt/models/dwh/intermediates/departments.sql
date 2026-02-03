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
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    etl_datetime
FROM {{ source('create', 'stg_create_company_departments') }}

{{ config(materialized='table') }}


SELECT
    a._id as user_id,
    a.name as user_name,
    a.username as username,
    a.password as password,
    a.email as company_email,
    a."staffCode" as staff_code,
    a."branchObjId" as branch_id,
    a."departmentObjId" as department_id,
    a."userPositionObjId" as position_id,
    {{ safe_parse_timestamp('a."createdAt"') }} as create_time,
    {{ safe_parse_timestamp('a."userUpdatedAt"') }} as update_time,
    a."userLevel" as user_level,
    a."userStatus" as user_status,
    a."isDeleted" as is_deleted,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_users') }} a
WHERE a."isDeleted" = 'No'


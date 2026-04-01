{{ config(materialized='table') }}


SELECT
    a._id as user_id,
    a.name as user_name,
    a.username as username,
    a.password as password,
    a.email as company_email,
    a."staffCode"::NUMERIC as staff_code,
    a."branchObjId" as branch_id,
    a."departmentObjId" as department_id,
    a."userPositionObjId" as position_id,
    a."userSubPositionObjId" as sub_position_id,
    {{ safe_parse_timestamp('a."createdAt"') }} as create_time,
    {{ safe_parse_timestamp('a."userUpdatedAt"') }} as update_time,
    {{ safe_parse_timestamp('a."expiresDate"') }} as expired_time,
    {{ safe_parse_multiple_dates('a."welcomeDay"') }} as welcome_day,
    a."jobObjId" as job_id,
    a."performanceFactor"::NUMERIC as performance_factor,
    a."userLevel" as user_level,
    a."userStatus" as user_status,
    a."isDeleted" as is_deleted,
    etl_datetime
FROM {{ source('create', 'stg_create_users') }} a
-- where a."isDeleted" = 'No'


{{ config(materialized='table') }}


SELECT
    a._id as user_id,
    a.name as user_name,
    {{ safe_parse_multiple_dates('b."birthDay"') }} as parsed_birthday,
    b.address as address,
    b.gender as gender,
    b."emailPersonal" as personal_email,
    b.mobile as mobile,
    a.username as username,
    a.password as password,
    a.email as company_email,
    a."staffCode" as staff_code,
    a."branchObjId" as branch_id,
    a."departmentObjId" as department_id,
    a."userPositionObjId" as position_id,
    -- to_date(nullif(a."welcomeDay",''), 'YYYY-MM-DD') as welcome_day,
    {{ safe_parse_multiple_dates('b."welcomeDay"') }} as welcome_day,

    -- to_date(nullif(b."probationDate",''), 'YYYY-MM-DD') as probation_date,
    {{ safe_parse_multiple_dates('b."probationDate"') }} as probation_date,
    {{ safe_parse_multiple_dates('b."quitDate"') }} as quit_date,
    {{ safe_parse_timestamp('a."createdAt"') }} as create_time,
    a."userLevel" as user_level,
    a."userStatus" as user_status,
    a."isDeleted" as is_deleted,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_users') }} a
LEFT JOIN {{ source('create', 'stg_user_infos') }} b
ON a."staffCode" = b."staffCode"
and a._id = b."userObjId"

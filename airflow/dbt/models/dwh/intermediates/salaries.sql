{{ config(materialized='table') }}

SELECT
    _id as salary_id,
    "userObjId" as user_id,
    insurance,
    "salaryBasic" as basic_salary,
    "totalSalary" as total_salary,
    "isDeleted" as is_deleted,
    status,
    "userInfoBlock" as user_info_block,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_salaries') }}
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
    TO_TIMESTAMP("createdAt",'YYYY-MM-DD HH24:MI:SS') as created_time,
    TO_TIMESTAMP("updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_salaries') }}
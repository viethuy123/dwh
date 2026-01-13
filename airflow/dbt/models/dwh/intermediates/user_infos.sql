{{ config(materialized='table') }}

with user_data as (
    SELECT
        _id as info_id,
        "userObjId" as user_id,
        "employeeID" as employee_id,
        "staffCode" as staff_code,
        "firstName" as first_name,
        "middleName" as middle_name,
        "lastName" as last_name,
        "emailCompany" as email_company,
        "emailPersonal" as email_personal,
        {{ safe_parse_timestamp('"birthDay"') }} as birthday,
        "gender" as gender,
        "mobile" as mobile,
        "address" as address,
        "userJobStatus" as user_job_status,
        {{ safe_parse_timestamp('"officialDate"') }} as official_date,
        {{ safe_parse_timestamp('"probationDate"') }} as probation_date,
        "organizationUnitID" as organization_unit_id,
        "organizationUnitName" as organization_unit_name,
        {{ safe_parse_timestamp('"internDate"') }} as intern_date,
        "calculateSeniority" as calculate_seniority,
        {{ safe_parse_timestamp('"quitDate"') }} as quit_date,
        {{ safe_parse_timestamp('"timekeepingDate"') }} as timekeeping_date,
        {{ safe_parse_timestamp('"welcomeDate"') }} as welcome_date,
        {{ safe_parse_timestamp('"createdAt"') }} as created_time,
        {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
        CURRENT_TIMESTAMP as etl_datetime
    FROM {{ source('create', 'stg_create_user_infos') }}
),

 ranked_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY staff_code ORDER BY created_time DESC) AS rn
    FROM user_data
)
SELECT * FROM ranked_data 
WHERE rn = 1


{{ config(materialized='table') }}

SELECT
    pod_id,
    user_id as member_id,
    department_id,
    user_role,
    effort,
    month_year,
    status,
    etl_datetime
FROM {{ ref('billable_efforts_approveds') }}
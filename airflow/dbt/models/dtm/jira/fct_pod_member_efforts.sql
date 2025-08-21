{{ config(materialized='table') }}

SELECT
    pod_id,
    user_id as member_id,
    department_id,
    user_role,
    effort,
    month_year,
    status
FROM {{ source('dwh', 'billable_efforts_approveds') }}
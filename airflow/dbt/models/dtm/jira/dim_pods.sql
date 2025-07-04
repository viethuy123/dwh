{{ config(materialized='table') }}

SELECT
    pod_id,
    project_code,
    project_name,
    project_size,
    jira_url,
    department_id,
    start_date,
    plan_uat_date,
    plan_release_date,
    final_release_date,
    pod_status,
    status
FROM {{ source('dwh', 'pods') }}
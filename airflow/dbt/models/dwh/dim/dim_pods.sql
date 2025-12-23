{{ config(materialized='table') }}

SELECT
    pod_id,
    project_code,
    project_name,
    project_type,
    project_size,
    project_rank,
    project_overview,
    project_category,
    jira_url,
    department_id,
    start_date,
    plan_uat_date,
    plan_release_date,
    final_release_date,
    pod_status,
    status,
    is_deleted
FROM {{ ref('pods') }}
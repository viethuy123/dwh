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
    substring(jira_url FROM '(?:\/browse\/|\/projects\/|\/project-config\/)([A-Z0-9]+)') AS code_jira,
    department_id,
    pm_id,
    sale_id,
    customer_id,
    domain,
    market_id,
    sub_pm_id,
    start_date,
    plan_uat_date,
    plan_release_date,
    final_release_date,
    pod_status,
    status,
    is_deleted,
    etl_datetime
FROM {{ ref('pods') }}
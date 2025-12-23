{{ config(materialized='table') }}


SELECT
    id,
    "name" as project_name,
    name_pm,
    name_br_se,
    "code" as project_code,
    location as project_branch,
    "scope" as project_scope,
    "type" as project_type,
    point_css as point_customer,
    point_comment,
    amount,
    summary,
    size as man_month,
    period as num_month,
    status,
    project_rank,
    team_size,
    "startDate" as start_time,
    "endDate" as end_time,
    "created_at" as create_time,
    "updated_at" as update_time
FROM {{ source('jisseki', 'stg_jisseki_projects') }}

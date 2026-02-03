{{ config(materialized='table') }}

-- có customer , category
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
    CASE 
        WHEN project_rank = 0 THEN 'A'
        WHEN project_rank = 1 THEN 'B'
        WHEN project_rank = 2 THEN 'C'
        WHEN project_rank = 3 THEN 'D'
        WHEN project_rank = 4 THEN 'E'
        ELSE 'Other'
    END AS project_rank,
    team_size,
    "startDate" as start_time,
    "endDate" as end_time,
    "created_at" as created_time,
    "updated_at" as updated_time,
    etl_datetime
FROM {{ source('jisseki', 'stg_jisseki_projects') }}

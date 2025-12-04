{{ config(materialized='table') }}


SELECT
    a.id as project_id,
    a.name as project_name,
    a.name_pm,
    a.name_br_se,
    a.code as project_code,
    a.location as branch_code_project,
    a.scope,
    a.type,
    CAST(a.amount AS DOUBLE PRECISION) as amount,
    a.summary,
    a."startDate" as start_day,
    a."endDate" as end_day,
    a.created_at,
    a.updated_at,
    a.size as project_MM,
    a.period as period_month,
    a.status,
    a.project_rank,
    a.team_size,

    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jisseki', 'stg_projects') }} a



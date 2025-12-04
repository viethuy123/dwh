{{ config(
    materialized='table',
    unique_key='project_key'
) }}
SELECT
    {{ generate_sk(['pd.project_id']) }} AS project_key,
    pd.project_id,
    {{ generate_sk(['pd.start_day']) }} AS start_day_key,
    {{ generate_sk(['pd.end_day']) }} AS end_day_key,
    pd.amount,
    pd.project_MM,
    pd.team_size,
    pd.period_month,
    pd.project_name,
    pd.project_code,
    pd.branch_code_project,
    pd.scope,
    pd.type,
    pd.summary,
    pd.name_pm,
    pd.name_br_se,
    pd.status,
    pd.project_rank,
    pd.start_day,
    pd.end_day,
    pd.created_at,
    pd.updated_at
    
FROM {{ source('dwh', 'jisseki_projects') }} pd
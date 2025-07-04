{{ config(materialized='table') }}


WITH project_values AS (
    SELECT 
        project_id,
        sum(coalesce(value,0) + coalesce(value1,0)) as project_value
    FROM {{source('dwh', 'project_profit_loss')}}
    GROUP BY project_id
)
SELECT 
    a.project_id,
    jira_project_id,
    project_name,
    project_jira_url,
    project_lead,
    project_status,
    location,
    scope,
    type,
    point_css,
    summary,
    size,
    period,
    team_size,
    start_date,
    end_date,
    b.project_value
FROM {{source('dwh', 'projects')}} a
LEFT JOIN project_values b
ON a.project_id = b.project_id
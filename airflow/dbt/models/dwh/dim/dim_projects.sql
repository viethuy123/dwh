{{ config(materialized='table') }}


WITH project_values AS (
    SELECT 
        project_id,
        sum(coalesce(value,0) + coalesce(value1,0)) as project_value
    FROM {{ ref('project_profit_loss') }}
    GROUP BY project_id
)
SELECT 
    a.*s
FROM {{ ref('projects') }} a

{{ config(materialized='table') }}


SELECT
    a.worklog_id,
    a.issue_id,
    b.jira_project_id,
    au.lower_user_name as worklog_author,
    a.worklog_description,
    a.start_time,
    a.time_worked,
    a.created_time,
    a.updated_time
FROM {{ source('dwh', 'jira_worklog') }} a
LEFT JOIN {{ source('dwh', 'jira_issues') }} b
ON a.issue_id = b.issue_id
LEFT JOIN {{ source('dwh', 'jira_app_user') }} au
ON a.worklog_author = au.user_key

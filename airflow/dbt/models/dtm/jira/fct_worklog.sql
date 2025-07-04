{{ config(materialized='table') }}


SELECT
    a.worklog_id,
    a.issue_id,
    b.jira_project_id,
    a.worklog_author,
    a.worklog_description,
    a.start_time,
    a.time_worked,
    a.created_time,
    a.updated_time
FROM {{ source('dwh', 'jira_worklog') }} a
LEFT JOIN {{ source('dwh', 'jira_issues') }} b
ON a.issue_id = b.issue_id
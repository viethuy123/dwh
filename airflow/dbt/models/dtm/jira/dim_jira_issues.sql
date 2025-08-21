{{ config(materialized='table') }}


SELECT
    a.issue_id,
    a.issue_assignee,
    e.priority_name as priority,
    b.type_name as type,
    c.resolution_name as resolution,
    a.resolution_date,
    d.status_name as status,
    a.due_date,
    a.time_original_estimate,
    a.time_estimate,	  
    a.time_spent,
    a.created_time,
    a.updated_time
FROM {{ source('dwh', 'jira_issues') }} a
LEFT JOIN {{ source('dwh', 'jira_issue_types') }} b
ON a.issue_type = b.type_id
LEFT JOIN {{ source('dwh', 'jira_issue_resolution') }} c
ON a.issue_resolution = c.resolution_id
LEFT JOIN {{ source('dwh', 'jira_issue_status') }} d      
ON a.issue_status = d.status_id
LEFT JOIN {{ source('dwh', 'jira_issue_priority') }} e
ON a.issue_priority = e.priority_id
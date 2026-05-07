{{ config(materialized='table') }}

with issue_data as (
    select iss.*,
    COALESCE(u_ass.lower_user_name, iss.issue_assignee) as assignee_email,
    COALESCE(u_rep.lower_user_name, iss.issue_reporter) as reporter_email
    from {{ ref('jira_issues') }} iss
        left join {{ ref('jira_app_user') }} u_ass
        on iss.issue_assignee = u_ass.user_key
        left join {{ ref('jira_app_user') }} u_rep
        on iss.issue_reporter = u_rep.user_key
)

SELECT
    a.issue_id,
    COALESCE(a.assignee_email, a.reporter_email) AS assignee_email,
    a.issue_number,
    a.jira_project_id,
    a.reporter_email as reporter_email,
    a.issue_creator,
    a.issue_reporter,
    a.issue_assignee,
    a.issue_summary,
    a.issue_description,
    e.priority_name as priority,
    a.issue_type as type_id,
    b.type_name as type,
    c.resolution_name as resolution,
    a.resolution_date,
    d.status_name as status,
    a.due_date,
    a.time_original_estimate,
    a.time_estimate,	  
    a.time_spent,
    a.created_time,
    a.updated_time,
    a.etl_datetime
FROM issue_data a
LEFT JOIN {{ ref('jira_issue_types') }} b
ON a.issue_type = b.type_id
LEFT JOIN {{ ref('jira_issue_resolution') }} c
ON a.issue_resolution = c.resolution_id
LEFT JOIN {{ ref('jira_issue_status') }} d      
ON a.issue_status = d.status_id
LEFT JOIN {{ ref('jira_issue_priority') }} e
ON a.issue_priority = e.priority_id
{{ config(
    materialized="materialized_view",
    on_configuration_change="apply",
    indexes=[
        {
            "columns": ["issue_id"],
            "unique": true,
            "type": "btree",
        }
    ]
) }}
WITH issue_data AS (
    select iss.* , co.custom_value as issue_level
    from {{ ref('dim_jira_issues_public') }} as iss 
    left join {{ source('dwh', 'jira_customfield_value') }} as cv
    on iss.issue_id = cv.issue_id
    left join {{ source('dwh', 'jira_customfield_option') }} as co
    on cv.custom_field = co.custom_field and cv.string_value::DOUBLE PRECISION = co.id
    where cv.custom_field in (12100,12632)  -- Example custom field ID
),
project_unique as (
    select 
        p.jira_project_id,
        p.project_name
    from {{ source('dwh', 'projects') }} as p
    group by 
        p.jira_project_id,
        p.project_name
)

SELECT 
    iss.*,
    COALESCE(u_latest.member_name, iss.issue_assignee) as assignee_name,
    COALESCE(u2_latest.member_name, iss.issue_reporter) as reporter_name,
    u_latest.staff_code as assignee_staff_code,
    u_latest.branch_name,
    u_latest.department_name,
    u_latest.position_name,
    u_latest.user_level,
    u_latest.user_status,
    p.project_name
FROM issue_data as iss
-- Lateral Join cho Assignee (u)
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('dim_users_public') }} as u_sub
    WHERE iss.issue_assignee = u_sub.member_email
      AND iss.created_time >= u_sub.create_time
    ORDER BY u_sub.create_time DESC -- Lấy phiên bản mới nhất
    LIMIT 1 
) as u_latest ON TRUE -- Luôn Join
-- Lateral Join cho Reporter (u2)
LEFT JOIN LATERAL (
    SELECT *
    FROM {{ ref('dim_users_public') }} as u2_sub
    WHERE iss.issue_reporter = u2_sub.member_email
      AND iss.created_time >= u2_sub.create_time
    ORDER BY u2_sub.create_time DESC -- Lấy phiên bản mới nhất
    LIMIT 1 
) as u2_latest ON TRUE -- Luôn Join
LEFT JOIN project_unique as p
  ON iss.jira_project_id = p.jira_project_id
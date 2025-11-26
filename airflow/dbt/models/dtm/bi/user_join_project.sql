{{ config(
    materialized='table', 
    -- unique_key=['issue_id', 'assignee_email'], 
    -- on_schema_change='append_new_columns' 
) }}

with get_level_task as (
    select cv.issue_id,
        co.id as option_id,
        co.custom_value
        from {{ source('dwh', 'jira_customfield_value') }} as cv
            left join {{ source('dwh', 'jira_customfield_option') }} as co
            on cv.custom_field = co.custom_field and cv.string_value::DOUBLE PRECISION = co.id
        where cv.custom_field in (12100,12632)
),
-- lấy level task từ custom field jira
 issue_data AS (
    select iss.* , 
    glt.option_id as issue_level_id,
    glt.custom_value as issue_level

    from {{ ref('dim_jira_issues_public') }} as iss 
    left join get_level_task as glt
    on iss.issue_id = glt.issue_id
    -- {% if is_incremental() %}
    --   AND (iss.updated_time) > (SELECT max((updated_time)) FROM {{ this }})
    -- {% endif %}
),
-- lấy thông tin project và role của user trong project
project_role as (
    select 
        r.role_name as role_name,
        ru.project_id,
        COALESCE(u.lower_user_name, ru.user_email) as user_email
    from {{ source('dwh', 'jira_project_role_actor') }} as ru
    join {{ source('dwh', 'jira_project_role') }} as r
        on ru.project_role_id = r.id
    left join {{ source('dwh', 'jira_app_user') }} as u
        on ru.user_email = u.user_key

),
project_name as (
    select 
        p.project_name,
        p.id as project_id
    from {{ source('dwh', 'jira_project') }} as p
    group by 
        p.project_name,
        p.id
),
project_role_with_weight as (
    SELECT
        pr.*,
        COUNT(pr.role_name) OVER (
            PARTITION BY pr.project_id, pr.user_email
        ) AS total_roles_per_user_project,
        1.0 / COUNT(pr.role_name) OVER (
            PARTITION BY pr.project_id, pr.user_email
        ) AS weight_factor
    FROM project_role AS pr
),
worklog_time as (
    select 
        issue_id,
        COALESCE(u.lower_user_name,worklog_author) as worklog_author,
        sum(time_worked) as total_time_worked
    from {{ source('dwh', 'jira_worklog') }}
    left join {{ source('dwh', 'jira_app_user') }} as u
        on worklog_author = u.user_key
    group by issue_id, COALESCE(u.lower_user_name,worklog_author)
)

SELECT 
    iss.issue_id,
    iss.issue_number,
    iss.jira_project_id,
    iss.issue_level,
    iss.issue_level_id,
    COALESCE(wlt.worklog_author, iss.assignee_email) as assignee_email,
    iss.reporter_email,
    iss.issue_summary,
    iss.priority,
    iss.type,
    iss.resolution,
    iss.status,
    iss.resolution_date,
    iss.due_date,
    iss.time_original_estimate,
    iss.time_estimate,
    iss.time_spent,
    iss.created_time,
    iss.updated_time,
    COALESCE(u_ass.member_name, wlt.worklog_author, iss.assignee_email) as assignee_name,
    COALESCE(u_re.member_name, iss.reporter_email) as reporter_name,
    u_ass.staff_code as assignee_staff_code,
    u_ass.branch_name,
    u_ass.branch_code,
    u_ass.department_name,
    u_ass.position_name,
    u_ass.user_level,
    u_ass.user_status,
    p.project_name,
    p.project_id,
    pr.role_name,
    pr.total_roles_per_user_project,
    COALESCE(pr.weight_factor, 1) as weight_factor,
    wlt.total_time_worked

FROM issue_data as iss
left join worklog_time as wlt
    on iss.issue_id = wlt.issue_id

left JOIN {{ ref('dim_users_public') }} as u_ass
    on COALESCE(wlt.worklog_author, iss.assignee_email) = u_ass.member_email
    AND date(iss.created_time) >= u_ass.create_date
    and date(iss.created_time) < u_ass.end_date
left JOIN {{ ref('dim_users_public') }} as u_re
    on iss.reporter_email = u_re.member_email
    AND date(iss.created_time) >= u_re.create_date
    and date(iss.created_time) < u_re.end_date
LEFT JOIN project_name as p
  ON iss.jira_project_id = p.project_id
LEFT JOIN project_role_with_weight as pr
  ON iss.jira_project_id = pr.project_id
  and COALESCE(wlt.worklog_author, iss.assignee_email) = pr.user_email
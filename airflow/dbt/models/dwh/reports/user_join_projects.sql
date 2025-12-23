{{ config(
    materialized='table'
) }}

with get_level_task as (
    select cv.issue_id,
        co.id as option_id,
        co.custom_value
        from {{ ref('jira_customfield_value') }} as cv
            left join {{ ref('jira_customfield_option') }} as co
            on cv.custom_field = co.custom_field and cv.string_value::DOUBLE PRECISION = co.id
        where cv.custom_field in (12100,12632)
),
-- lấy level task từ custom field jira
 issue_data AS (
    select iss.* , 
    glt.option_id as issue_level_id,
    glt.custom_value as issue_level

    from {{ ref('dim_jira_issues') }} as iss 
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
    from {{ ref('jira_project_role_actor') }} as ru
    join {{ ref('jira_project_role') }} as r
        on ru.project_role_id = r.id
    left join {{ ref('jira_app_user') }} as u
        on ru.user_email = u.user_key

),
project_name as (
    select 
        p.project_name,
        p.id as project_id
    from {{ ref('jira_project') }} as p
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
        worklog_author,
        start_time,
        sum(time_worked) as total_time_worked
    from {{ ref('fct_worklogs') }}
    group by issue_id, worklog_author, start_time
),
data_worklog as (
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
        wlt.start_time,
        wlt.total_time_worked
        -- pr.role_name,
        -- pr.total_roles_per_user_project,
        -- COALESCE(pr.weight_factor, 1) as weight_factor,
        -- wlt.total_time_worked * COALESCE(pr.weight_factor, 1) as time_worked_s,
        -- (wlt.total_time_worked * COALESCE(pr.weight_factor, 1))/3600 as time_worked_h

    FROM issue_data as iss
    left join worklog_time as wlt
        on iss.issue_id = wlt.issue_id
),

user_not_duplicate as ( 
    SELECT 
        *
    FROM {{ ref('dim_members') }}
    WHERE count_email_duplicates = 1
)

SELECT 
    iss.issue_id::TEXT,
    iss.issue_number::TEXT,
    iss.jira_project_id::TEXT,
    iss.issue_level,
    iss.issue_level_id::TEXT,
    iss.assignee_email,
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
    COALESCE(u_ass.member_name, u_nd.member_name, iss.assignee_email) as assignee_name,
    COALESCE(u_re.member_name, iss.reporter_email) as reporter_name,
    COALESCE(u_ass.member_name, u_nd.member_name) as member_name,
    COALESCE(u_ass.staff_code, u_nd.staff_code) as assignee_staff_code,
    COALESCE(u_ass.branch_name, u_nd.branch_name) as assignee_branch_name,
    COALESCE(u_ass.branch_code, u_nd.branch_code) as assignee_branch_code,
    COALESCE(u_ass.department_name, u_nd.department_name) as assignee_department_name,
    COALESCE(u_ass.position_name, u_nd.position_name) as assignee_position_name,
    COALESCE(u_ass.user_level, u_nd.user_level) as assignee_user_level,
    COALESCE(u_ass.user_status, u_nd.user_status) as assignee_user_status,
    p.project_name,
    pr.role_name,
    pr.total_roles_per_user_project,
    COALESCE(pr.weight_factor, 1) as weight_factor,
    iss.start_time,
    iss.total_time_worked * COALESCE(pr.weight_factor, 1) as time_worked_s,
    (iss.total_time_worked * COALESCE(pr.weight_factor, 1))/3600 as time_worked_h

FROM data_worklog as iss


-- ưu tiên lấy data của worklog_author nếu có, nếu không thì lấy assignee_email
left JOIN {{ ref('dim_members') }} as u_ass
    on iss.assignee_email = u_ass.member_email
    AND COALESCE(date(iss.start_time), date(iss.created_time)) >= u_ass.create_date_used
    and COALESCE(date(iss.start_time), date(iss.created_time)) <= u_ass.end_date
-- có thể user sẽ k thỏa với khoảng ngày nên sẽ dùng 1 join k điều kiện ngày cho chắc
left join user_not_duplicate as u_nd
    on iss.assignee_email  = u_nd.member_email
left JOIN {{ ref('dim_members') }} as u_re
    on iss.reporter_email = u_re.member_email
    AND COALESCE(date(iss.start_time), date(iss.created_time)) >= u_re.create_date_used
    and COALESCE(date(iss.start_time), date(iss.created_time)) <= u_re.end_date
LEFT JOIN project_name as p
  ON iss.jira_project_id = p.project_id
LEFT JOIN project_role_with_weight as pr
  ON iss.jira_project_id = pr.project_id
  and iss.assignee_email = pr.user_email
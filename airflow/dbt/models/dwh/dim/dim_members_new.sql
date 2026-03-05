{{ config(materialized='table') }}



WITH user_data AS (
    SELECT 
        *
    FROM {{ ref('users') }}
),
user_info_data AS (
    SELECT 
        *
    FROM {{ ref('users_infos_tranforms') }}
),
user_all as (
    select u.*,
    COALESCE(ui.official_date_use,ui.probation_date_use,ui.intern_date_use,ui.created_at) as official_date_use,
    ui.probation_date_use,
    ui.intern_date_use,
    ui.created_at,
    ui.quit_date_use,
    ui.end_date as period_end_date_two_user
    
    from user_data u
        join user_info_data ui
        on u.user_id = ui.user_id
),
tranform_date as (
    select * ,
    CASE 
        WHEN (user_status IN ('Inactivity', 'null') OR user_status IS NULL) AND official_date_use is null and quit_date_use is null
            THEN coalesce(probation_date_use, intern_date_use, created_at)
        
        ELSE quit_date_use
    END as quit_date_new
    from user_all
),

-- user sẽ có ngày tạo và kết thúc , nhưng trạng thái inactive vẫn cần chỉnh lại để biết nghỉ thời gian nào
-- sẽ chuyển create_time về đầu tháng vì case trên đã tạo cuối tháng , k sợ trùng
cleaned_date as (
    select * ,
    date(DATE_TRUNC('month', official_date_use)) as official_date,
    date(DATE_TRUNC('month', quit_date_new)) as quit_date_original
    from tranform_date
),

user_log AS (
    SELECT 
        u.lower_user_name as email,
        max(DATE_TRUNC('month',w.start_time)::DATE) as date
    FROM {{ ref('jira_worklog') }} w
    LEFT JOIN {{ ref('jira_app_user') }} u
    on w.worklog_author = u.user_key
    LEFT JOIN {{ ref('users') }} du
    on u.lower_user_name = du.company_email
    where (du.user_status IN ('Inactivity', 'null') or du.user_status is null)
    group by u.lower_user_name
),
user_jira_issues AS (
    SELECT 
        i.assignee_email as email,
        max(DATE_TRUNC('month',i.created_time)::DATE) as date
    FROM {{ ref('dim_jira_issues') }} i
    LEFT JOIN {{ ref('users') }} du
    on i.assignee_email = du.company_email
    where (du.user_status IN ('Inactivity', 'null') or du.user_status is null)
    group by i.assignee_email

),
user_pod AS (
    SELECT 
        u.company_email as email,
        max((p.month_year || '-01')::DATE) AS date
    FROM {{ ref('billable_efforts_approveds') }} p
    LEFT JOIN {{ ref('users') }} u
    on p.user_id = u.user_id
    where (u.user_status IN ('Inactivity', 'null') or u.user_status IS NULL)
    and effort != 0
    and p."is_deleted" = 'No'
    group by u.company_email
),
all_data_log as (
    select * from user_log
    union
    select * from user_pod
    union
    select * from user_jira_issues
),
-- lấy ngày cuối cùng user có ghi nhận trong hệ thống , chỉ lấy user inactive, null
max_date_user as (
    select 
        email,
        max(date) as max_date
    from all_data_log
    group by email
),

change_end_date_inactive_user as (
    select 
        cu.*,
        case 
            when period_end_date_two_user is null 
                 and (cu.user_status IN ('Inactivity', 'null') or cu.user_status IS NULL)
            then (date_trunc('month', coalesce(mu.max_date, cu.official_date)) + interval '1 month - 1 day')::DATE
        end as end_date_1
    from cleaned_date cu
    left join max_date_user mu
           on cu.company_email = mu.email
),

logic_date as (
    select
        *,
        CASE
            when (user_status IN ('Inactivity', 'null') or user_status IS NULL)
                then COALESCE(quit_date_original, end_date_1, official_date)
        END as end_date_raw
    from change_end_date_inactive_user
),

_final as (
    select 
        *,
        CASE
            when end_date_raw is not null and end_date_raw < official_date then official_date
            else end_date_raw
        END as end_date
    from logic_date
)


SELECT
    a.user_id as member_id,
    a.user_name as member_name,
    a.company_email as member_email,
    a.staff_code,
    b.branch_name,
    b.branch_code,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status,
    date(a.create_time) as create_date,
    a.official_date,
    date(DATE_TRUNC('month', a.create_time)) as create_date_used,
    a.end_date,
    COUNT(*) OVER (
        PARTITION BY a.company_email
    ) AS count_email_duplicates,
    a.etl_datetime
FROM _final a
LEFT JOIN {{ ref('branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ ref('departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ ref('user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'
and a.branch_code != 'CNTO'
and a.department_id != '60c0889f1b7b381078ad66ee'
-- and a.staff_code is not NULL 
-- and a.user_status not IN ('Inactivity', 'null')

GROUP BY
    member_id,
    member_name,
    member_email,
    staff_code,
    b.branch_name,
    b.branch_code,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status,
    date(a.create_time),
    date(DATE_TRUNC('month', a.create_time)),
    a.end_date,
    a.official_date,
    a.etl_datetime

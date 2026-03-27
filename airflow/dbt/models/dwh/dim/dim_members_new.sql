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
    COALESCE(ui.official_date_use,ui.probation_date_use,ui.intern_date_use,ui.created_at) as official_date,
    ui.probation_date_use,
    ui.intern_date_use,
    ui.birth_day,
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
        WHEN (user_status IN ('Inactivity', 'null') OR user_status IS NULL) AND official_date is null and quit_date_use is null
            THEN coalesce(probation_date_use, intern_date_use, created_at)
        
        ELSE quit_date_use
    END as quit_date_original
    from user_all
),

cleaned_date as (
    select * 
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

change_date as (
    select 
        *,
        CASE
            when end_date_raw is not null and end_date_raw < official_date then official_date
            else end_date_raw
        END as end_date,
        CASE 
            WHEN create_time < official_date THEN date_trunc('month', create_time)::date
            ELSE date_trunc('month', official_date)::date
        END as create_date_used
    from logic_date
),

_final as (
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
    a.official_date::date as official_date,
    a.probation_date_use::date as probation_date,
    a.intern_date_use::date as intern_date,
    a.welcome_day::date as welcome_day,
    a.birth_day,
    COALESCE(
        EXTRACT(YEAR FROM a.official_date) - EXTRACT(YEAR FROM a.birth_day), 
        0
    ) AS age_at_hire,
    a.create_date_used,
    a.end_date::date as end_date,
    COUNT(*) OVER (
        PARTITION BY a.company_email
    ) AS count_email_duplicates,
    a.etl_datetime
FROM change_date a
LEFT JOIN {{ ref('branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ ref('departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ ref('user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null'
-- and a.branch_id != '607ce230e7fbdb31ac5ed2d0'
-- and a.department_id != '60c0889f1b7b381078ad66ee'
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
    -- date(DATE_TRUNC('month', a.create_time)),
    a.create_date_used,
    a.end_date,
    a.official_date,
    a.probation_date_use,
    a.intern_date_use,
    a.welcome_day,
    a.birth_day,
    age_at_hire,
    a.etl_datetime

)
select 
    member_id,
    member_name,
    member_email,
    staff_code,
    COALESCE(NULLIF(branch_name, 'NO'), 'Unknown') AS branch_name,
    COALESCE(NULLIF(branch_code, 'NO'), 'Unknown') AS branch_code,
    COALESCE(NULLIF(department_name, 'NO'), 'Unknown') AS department_name,
    CASE 
        WHEN department_name ILIKE '%DU%' THEN
            REGEXP_REPLACE(department_name, '\.?DU.*', '')
        ELSE department_name
        END as department_group,
    COALESCE(NULLIF(position_name, 'NO'), 'Unknown') AS position_name,
    COALESCE(NULLIF(user_level, 'NO'), 'FRESHER') AS user_level,
    COALESCE(NULLIF(user_status, 'NO'), 'Unknown') AS user_status,
    CASE 
        -- INTERN / TRAINEE
        WHEN position_name ILIKE '%intern%' 
        OR position_name ILIKE '%thử việc%' 
        OR position_name ILIKE '%học việc%' 
        OR position_name ILIKE '%fresher%' 
        THEN 'INTERN_TRAINEE'

        -- MANAGEMENT
        WHEN position_name ILIKE '%manager%' 
        OR position_name ILIKE '%director%' 
        OR position_name ILIKE '%head%' 
        OR position_name ILIKE '%leader%' 
        OR position_name ILIKE '%ceo%' 
        OR position_name ILIKE '%cto%' 
        THEN 'MANAGEMENT'

        -- ENGINEERING
        WHEN position_name ILIKE '%developer%' 
        OR position_name ILIKE '%engineer%' 
        OR position_name ILIKE '%data%' 
        OR position_name ILIKE '%ai%' 
        OR position_name ILIKE '%machine learning%' 
        OR position_name ILIKE '%tester%' 
        OR position_name ILIKE '%qa%' 
        OR position_name ILIKE '%devops%' 
        OR position_name ILIKE '%infra%' 
        OR position_name ILIKE '%cloud%' 
        THEN 'ENGINEERING'

        -- PRODUCT / BA
        WHEN position_name ILIKE '%ba%' 
        OR position_name ILIKE '%business analyst%' 
        OR position_name ILIKE '%product%' 
        THEN 'PRODUCT_BA'

        -- SALES
        WHEN position_name ILIKE '%sale%' 
        OR position_name ILIKE '%account%' 
        OR position_name ILIKE '%business development%' 
        OR position_name ILIKE '%pre-sales%' 
        THEN 'SALES'

        -- MARKETING
        WHEN position_name ILIKE '%marketing%' 
        OR position_name ILIKE '%mkt%' 
        OR position_name ILIKE '%content%' 
        OR position_name ILIKE '%seo%' 
        THEN 'MARKETING'

        -- HR / ADMIN
        WHEN position_name ILIKE '%hr%' 
        OR position_name ILIKE '%admin%' 
        OR position_name ILIKE '%accountant%' 
        OR position_name ILIKE '%ta%' 
        OR position_name ILIKE '%ga%' 
        THEN 'HR_ADMIN'

        -- OPERATION
        WHEN position_name ILIKE '%project%' 
        OR position_name ILIKE '%delivery%' 
        OR position_name ILIKE '%operation%' 
        OR position_name ILIKE '%support%' 
        THEN 'OPERATION'

        ELSE 'OTHER'
        END as position_group,
    create_date,
    official_date,
    probation_date,
    intern_date,
    welcome_day,
    birth_day,
    age_at_hire,
    create_date_used,
    end_date,
    count_email_duplicates,
    etl_datetime
 from _final


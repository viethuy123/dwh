{{ config(materialized='table') }}



WITH 

priority_status AS (
    SELECT 
        u.*,
        CASE 
            WHEN u.user_status IS NOT NULL AND u.user_status != '' 
            AND u.user_status NOT ILIKE '%Inac%' 
            THEN 1
            ELSE 0  -- Inactive/Null/Empty Status
        END AS sort_priority -- Cột ưu tiên

    FROM {{ ref('members_snapshot') }} u
    WHERE 
    NOT (user_status ILIKE '%Inac%' and u.dbt_updated_at > (SELECT MIN(dbt_updated_at) FROM {{ ref('members_snapshot') }}))
),
create_date_used AS (
    SELECT 
        *,
        CASE 
            WHEN dbt_valid_from > (SELECT MIN(dbt_valid_from) FROM priority_status)
            THEN date(dbt_valid_from)
            ELSE date(create_time)
        END AS create_date_used
    FROM priority_status
),

get_end_date AS (
    SELECT 
        a.*,
        -- SẮP XẾP: Các bản ghi sort_priority=0 (Inactive) được xếp trước theo thời gian, 
        -- Bản ghi sort_priority=1 (Active) được đẩy xuống cuối.
        CASE 
            WHEN LEAD(create_date_used, 1, TIMESTAMP '2999-12-31') OVER (
                PARTITION BY company_email
                ORDER BY sort_priority ASC, create_date_used ASC , dbt_valid_from ASC
            ) = TIMESTAMP '2999-12-31' 
            THEN DATE '2999-12-31'
            ELSE (DATE_TRUNC('day', 
                LEAD(create_date_used, 1, TIMESTAMP '2999-12-31') OVER (
                    PARTITION BY company_email
                    ORDER BY sort_priority ASC, create_date_used ASC ,dbt_valid_from ASC
                )
            ) - INTERVAL '1 day')::DATE
        END AS end_date_1

    FROM create_date_used a

),
--  dùng cho case 1 email có 2 người dùng
cleaned_data AS (
    SELECT
        *
    FROM get_end_date
    WHERE 
        date(create_date_used) < end_date_1
        OR end_date_1 = DATE('2999-12-31')
),

-- user sẽ có ngày tạo và kết thúc , nhưng trạng thái inactive vẫn cần chỉnh lại để biết nghỉ thời gian nào
cleaned_users as (
    select *
    from cleaned_data
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
    where du.user_status IN ('Inactivity', 'null')
    group by u.lower_user_name
),
user_pod AS (
    SELECT 
        u.company_email as email,
        max((p.month_year || '-01')::DATE) AS date
    FROM {{ ref('billable_efforts_approveds') }} p
    LEFT JOIN {{ ref('users') }} u
    on p.user_id = u.user_id
    where u.user_status IN ('Inactivity', 'null')
    and effort != 0
    and p."is_deleted" = 'No'
    group by u.company_email
),
all_data_log as (
    select * from user_log
    union
    select * from user_pod
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
            
            when (mu.max_date is NULL)
                AND date(cu.end_date_1) = '2999-12-31' and (cu.user_status IN ('Inactivity', 'null') or cu.user_status IS NULL)
                then cu.create_date_used::DATE
            when (mu.max_date is not NULL)
                AND date(cu.end_date_1) = '2999-12-31' and (cu.user_status IN ('Inactivity', 'null') or cu.user_status IS NULL)
                then mu.max_date
        end as end_date_2
    from cleaned_users cu
    left join max_date_user mu
    on cu.company_email = mu.email
),
_final as (
    select 
        *,
        COALESCE(end_date_2, end_date_1) as end_date
    from change_end_date_inactive_user
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
    a.create_date_used,
    a.end_date
FROM _final a
LEFT JOIN {{ ref('branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ ref('departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ ref('user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'
and a.staff_code is not NULL 
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
    a.create_date_used,
    a.end_date

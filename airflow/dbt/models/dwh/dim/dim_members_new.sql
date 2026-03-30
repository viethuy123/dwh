{{ config(materialized='table') }}

WITH user_base AS (
    SELECT
        u.user_id,
        u.user_name,
        u.company_email,
        u.staff_code,
        u.branch_id,
        u.department_id,
        u.position_id,
        u.user_level,
        u.user_status,
        u.create_time,
        u.welcome_day,
        u.etl_datetime,
        -- Resolve official_date ngay tại đây
        COALESCE(
            ui.official_date_use,
            ui.probation_date_use,
            ui.intern_date_use,
            ui.created_at
        )                          AS official_date,
        ui.probation_date_use,
        ui.intern_date_use,
        ui.birth_day,
        ui.created_at,
        ui.quit_date_use,
        ui.end_date                AS period_end_date
    FROM {{ ref('users') }} u
    JOIN {{ ref('users_infos_tranforms') }} ui
        ON u.user_id = ui.user_id
    WHERE u.company_email IS NOT NULL
      AND u.company_email != 'null'
),

-- ============================================================
-- LAYER 2: Odoo job/role (fix: dùng DISTINCT thay vì GROUP BY)
-- ============================================================
odoo_job AS (
    SELECT DISTINCT
        e.employee_code,
        COALESCE(
            {{ parse_python_json('job_info.name_json') }}->>'vi_VN',
            {{ parse_python_json('job_info.name_json') }}->>'en_US',
            'unknown'
        ) AS job_name,
        COALESCE(
            {{ parse_python_json('role_info.name_json') }}->>'vi_VN',
            {{ parse_python_json('role_info.name_json') }}->>'en_US',
            'unknown'
        ) AS role_name
    FROM {{ ref('odoo_hr_employee') }} e
    JOIN {{ ref('odoo_hr_job') }} job_info
        ON e.job_id = job_info.job_id
    JOIN {{ ref('odoo_hr_job') }} role_info
        ON job_info.group_role_id = role_info.job_id
),

-- ============================================================
-- LAYER 3: Last active date cho inactive users
-- ============================================================
last_active_date AS (
    -- Worklog
    SELECT u.lower_user_name AS email,
           MAX(DATE_TRUNC('month', w.start_time)::DATE) AS last_date
    FROM {{ ref('jira_worklog') }} w
    JOIN {{ ref('jira_app_user') }} u ON w.worklog_author = u.user_key
    JOIN {{ ref('users') }} du        ON u.lower_user_name = du.company_email
    WHERE du.user_status IN ('Inactivity', 'null') OR du.user_status IS NULL
    GROUP BY u.lower_user_name

    UNION ALL

    -- Jira issues
    SELECT i.assignee_email AS email,
           MAX(DATE_TRUNC('month', i.created_time)::DATE) AS last_date
    FROM {{ ref('dim_jira_issues') }} i
    JOIN {{ ref('users') }} du ON i.assignee_email = du.company_email
    WHERE du.user_status IN ('Inactivity', 'null') OR du.user_status IS NULL
    GROUP BY i.assignee_email

    UNION ALL

    -- POD efforts
    SELECT u.company_email AS email,
           MAX((p.month_year || '-01')::DATE) AS last_date
    FROM {{ ref('billable_efforts_approveds') }} p
    JOIN {{ ref('users') }} u ON p.user_id = u.user_id
    WHERE (u.user_status IN ('Inactivity', 'null') OR u.user_status IS NULL)
      AND p.effort::NUMERIC != 0
      AND p.is_deleted = 'No'
    GROUP BY u.company_email
),

max_last_active AS (
    SELECT email, MAX(last_date) AS max_date
    FROM last_active_date
    GROUP BY email
),

-- ============================================================
-- LAYER 4: Resolve end_date logic
-- ============================================================
date_resolution AS (
    SELECT
        u.*,
        IS_INACTIVE.flag                                AS is_inactive,
        -- quit_date: ưu tiên quit_date_use, fallback về ngày sớm nhất có data
        CASE
            WHEN IS_INACTIVE.flag
             AND u.official_date IS NULL
             AND u.quit_date_use IS NULL
            THEN COALESCE(u.probation_date_use, u.intern_date_use, u.created_at)
            ELSE u.quit_date_use
        END                                             AS quit_date_resolved,
        -- end_date từ last active (nếu chưa có period_end_date)
        CASE
            WHEN IS_INACTIVE.flag AND u.period_end_date IS NULL
            THEN (
                DATE_TRUNC('month', COALESCE(mla.max_date, u.official_date))
                + INTERVAL '1 month - 1 day'
            )::DATE
        END                                             AS end_date_from_activity
    FROM user_base u
    CROSS JOIN LATERAL (
        SELECT (u.user_status IN ('Inactivity', 'null') OR u.user_status IS NULL) AS flag
    ) IS_INACTIVE
    LEFT JOIN max_last_active mla ON u.company_email = mla.email
),

final_dates AS (
    SELECT
        *,
        -- end_date cuối cùng
        CASE
            WHEN is_inactive THEN
                GREATEST(
                    COALESCE(quit_date_resolved, end_date_from_activity, official_date),
                    official_date  -- đảm bảo end_date không nhỏ hơn official_date
                )
        END                                             AS end_date,
        -- create_date_used
        CASE
            WHEN create_time < official_date
            THEN DATE_TRUNC('month', create_time)::DATE
            ELSE DATE_TRUNC('month', official_date)::DATE
        END                                             AS create_date_used
    FROM date_resolution
),

-- ============================================================
-- LAYER 5: Assemble dim
-- ============================================================
dim_assembled AS (
    SELECT
        -- Keys
        fd.user_id                                          AS member_id,
        fd.staff_code,
        fd.company_email                                    AS member_email,

        -- Descriptors
        fd.user_name                                        AS member_name,
        COALESCE(NULLIF(b.branch_name,   'NO'), 'Unknown') AS branch_name,
        COALESCE(NULLIF(b.branch_code,   'NO'), 'Unknown') AS branch_code,
        COALESCE(NULLIF(dep.department_name,'NO'),'Unknown')AS division_name,
        COALESCE(NULLIF(pos.position_name,'NO'), 'Unknown') AS position_name,
        COALESCE(NULLIF(fd.user_level,   'NO'), 'FRESHER') AS user_level,
        COALESCE(NULLIF(fd.user_status,  'NO'), 'Unknown') AS user_status,
        COALESCE(NULLIF(oj.job_name,     'NO'), 'Unknown') AS job_name,
        COALESCE(NULLIF(oj.role_name,    'NO'), 'Unknown') AS role_name,

        -- Dates
        DATE(fd.create_time)                                AS create_date,
        fd.official_date::DATE                              AS official_date,
        fd.probation_date_use::DATE                         AS probation_date,
        fd.intern_date_use::DATE                            AS intern_date,
        fd.welcome_day::DATE                                AS welcome_day,
        fd.birth_day,
        fd.end_date,
        fd.create_date_used,

        -- Metrics
        COALESCE(
            EXTRACT(YEAR FROM fd.official_date)
            - EXTRACT(YEAR FROM fd.birth_day),
            0
        )::INT                                              AS age_at_hire,

        -- DQ
        COUNT(*) OVER (PARTITION BY fd.company_email)       AS count_email_duplicates,

        fd.etl_datetime
    FROM final_dates fd
    LEFT JOIN {{ ref('branches') }}       b   ON fd.branch_id     = b.branch_id
    LEFT JOIN {{ ref('departments') }}    dep ON fd.department_id  = dep.department_id
    LEFT JOIN {{ ref('user_positions') }} pos ON fd.position_id    = pos.position_id
    LEFT JOIN odoo_job                    oj  ON fd.staff_code     = oj.employee_code
)

-- ============================================================
-- LAYER 6: Business classifications (tách khỏi join để dễ maintain)
-- ============================================================
SELECT
    member_id,
    member_name,
    member_email,
    staff_code,
    branch_name,
    branch_code,
    division_name,

    -- Division group: tách DU suffix
    CASE
        WHEN division_name ILIKE '%DU%'
        THEN TRIM(REGEXP_REPLACE(division_name, '\.?DU.*', ''))
        ELSE division_name
    END                                                     AS division_group,

    position_name,
    user_level,
    user_status,

    -- Position group classification
    CASE
        WHEN position_name ILIKE ANY(ARRAY['%intern%','%thử việc%','%học việc%','%fresher%'])
            THEN 'INTERN_TRAINEE'
        WHEN position_name ILIKE ANY(ARRAY['%manager%','%director%','%head%','%leader%','%ceo%','%cto%'])
            THEN 'MANAGEMENT'
        WHEN position_name ILIKE ANY(ARRAY['%developer%','%engineer%','%data%','%ai%',
                                           '%machine learning%','%tester%','%qa%',
                                           '%devops%','%infra%','%cloud%'])
            THEN 'ENGINEERING'
        WHEN position_name ILIKE ANY(ARRAY['%ba%','%business analyst%','%product%'])
            THEN 'PRODUCT_BA'
        WHEN position_name ILIKE ANY(ARRAY['%sale%','%account%','%business development%','%pre-sales%'])
            THEN 'SALES'
        WHEN position_name ILIKE ANY(ARRAY['%marketing%','%mkt%','%content%','%seo%'])
            THEN 'MARKETING'
        WHEN position_name ILIKE ANY(ARRAY['%hr%','%admin%','%accountant%','%ta%','%ga%'])
            THEN 'HR_ADMIN'
        WHEN position_name ILIKE ANY(ARRAY['%project%','%delivery%','%operation%','%support%'])
            THEN 'OPERATION'
        ELSE 'OTHER'
    END                                                     AS position_group,

    job_name,
    role_name,
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

FROM dim_assembled
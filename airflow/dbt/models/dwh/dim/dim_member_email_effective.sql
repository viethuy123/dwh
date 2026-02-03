{{ config(materialized='table') }}

WITH priority_status AS (
    SELECT 
        u.*,

        -- Active ưu tiên sau, inactive đóng trước
        CASE 
            WHEN u.user_status IS NOT NULL 
             AND u.user_status != '' 
             AND u.user_status NOT ILIKE '%Inac%' 
            THEN 1
            ELSE 0
        END AS sort_priority

    FROM {{ ref('members_snapshot') }} u
    WHERE 
    NOT (user_status ILIKE '%Inac%' and u.dbt_updated_at > (SELECT MIN(dbt_updated_at) FROM {{ ref('members_snapshot') }}))
),

/* =====================================================
   1. TẠO TIMELINE EMAIL (KHÔNG OVERLAP)
===================================================== */
get_end_date AS (
    SELECT 
        a.*,

        CASE 
            WHEN LEAD(create_time, 1, TIMESTAMP '2999-12-31') OVER (
                PARTITION BY company_email
                ORDER BY sort_priority ASC, create_time ASC
            ) = TIMESTAMP '2999-12-31'
            THEN DATE '2999-12-31'
            ELSE (
                DATE_TRUNC(
                    'month',
                    LEAD(create_time, 1, TIMESTAMP '2999-12-31') OVER (
                        PARTITION BY company_email
                        ORDER BY sort_priority ASC, create_time ASC
                    )
                ) - INTERVAL '1 day'
            )::DATE
        END AS end_date_1

    FROM priority_status a
),

/* =====================================================
   2. LOẠI RECORD NGƯỢC THỜI GIAN
===================================================== */
cleaned_data AS (
    SELECT *
    FROM get_end_date
    WHERE DATE(create_time) < end_date_1
       OR end_date_1 = DATE '2999-12-31'
),

/* =====================================================
   3. CHUẨN HÓA CREATE DATE VỀ ĐẦU THÁNG
===================================================== */
cleaned_users AS (
    SELECT 
        *,
        DATE_TRUNC('month', create_time)::DATE AS create_date_used
    FROM cleaned_data
),

/* =====================================================
   4. LẤY ACTIVITY CUỐI CÙNG (WORKLOG + POD)
===================================================== */
user_log AS (
    SELECT 
        u.lower_user_name AS email,
        MAX(DATE_TRUNC('month', w.start_time)::DATE) AS date
    FROM {{ ref('jira_worklog') }} w
    JOIN {{ ref('jira_app_user') }} u
      ON w.worklog_author = u.user_key
    GROUP BY u.lower_user_name
),

user_pod AS (
    SELECT 
        u.company_email AS email,
        MAX((p.month_year || '-01')::DATE) AS date
    FROM {{ ref('billable_efforts_approveds') }} p
    JOIN {{ ref('users') }} u
      ON p.user_id = u.user_id
    WHERE p.effort != 0
      AND p.is_deleted = 'No'
    GROUP BY u.company_email
),

all_data_log AS (
    SELECT * FROM user_log
    UNION
    SELECT * FROM user_pod
),

max_date_user AS (
    SELECT 
        email,
        MAX(date) AS max_date
    FROM all_data_log
    GROUP BY email
),

/* =====================================================
   5. LOGIC INACTIVE THEO ĐÚNG CÁCH BẠN ĐANG DÙNG
===================================================== */
change_end_date_inactive_user AS (
    SELECT 
        cu.*,

        CASE 
            -- Inactive + không activity + không expired
            WHEN cu.expired_time IS NULL
             AND mu.max_date IS NULL
             AND cu.end_date_1 = DATE '2999-12-31'
             AND (cu.user_status ILIKE '%Inac%' OR cu.user_status IS NULL)
            THEN (DATE_TRUNC('month', cu.create_time) 
                  + INTERVAL '1 month - 1 day')::DATE

            -- Activity sau expired hoặc expired null → lấy activity
            WHEN (mu.max_date > DATE_TRUNC('month', cu.expired_time)::DATE 
                  OR cu.expired_time IS NULL)
             AND cu.end_date_1 = DATE '2999-12-31'
             AND (cu.user_status ILIKE '%Inac%' OR cu.user_status IS NULL)
            THEN (DATE_TRUNC('month', mu.max_date) 
                  + INTERVAL '1 month - 1 day')::DATE

            -- Activity trước expired → chặn theo expired
            WHEN (mu.max_date <= DATE_TRUNC('month', cu.expired_time)::DATE 
                  OR mu.max_date IS NULL)
             AND cu.end_date_1 = DATE '2999-12-31'
             AND (cu.user_status ILIKE '%Inac%' OR cu.user_status IS NULL)
            THEN (DATE_TRUNC('month', cu.expired_time) 
                  + INTERVAL '1 month - 1 day')::DATE
        END AS end_date_2

    FROM cleaned_users cu
    LEFT JOIN max_date_user mu
      ON cu.company_email = mu.email
),

/* =====================================================
   6. END DATE CUỐI (GIỮ LOGIC CỦA BẠN)
===================================================== */
_final AS (
    SELECT 
        *,
        COALESCE(end_date_2, end_date_1) AS end_date
    FROM change_end_date_inactive_user
)

SELECT
{#  {{ dbt_utils.generate_surrogate_key([
        'company_email',
        'create_date_used'
    ]) }} AS member_email_sk, #}
    user_id                         AS member_id,
    user_name                       AS member_name,
    company_email                   AS member_email,
    staff_code,
    branch_id,
    department_id,
    position_id,
    user_level,
    user_status,

    DATE(create_time)               AS create_date,
    create_date_used,
    end_date,

    COUNT(*) OVER (
        PARTITION BY company_email
    ) AS count_email_duplicates,
    etl_datetime

FROM _final
WHERE company_email IS NOT NULL
  AND company_email != 'null'
  AND company_email NOT LIKE 'Inactive%'

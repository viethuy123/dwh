{{ config(
    materialized='table',
    indexes=[
      {'columns': ['report_date']},
      {'columns': ['member_id']},
      {'columns': ['member_code']},
      {'columns': ['report_date', 'member_status']},
    ]
    
    ) }}

WITH education_comprehensive AS (

    SELECT
        member_id,
        school_name,
        academic_level,
        degree_name,
        degree_group,
        graduation_rating,
        graduation_rating_group,
        graduation_year,

        ROW_NUMBER() OVER (
            PARTITION BY member_id
            ORDER BY record_created_at DESC, graduation_year DESC
        ) AS edu_rank

    FROM {{ ref('fct_member_education') }}

),

highest_education AS (

    SELECT *
    FROM education_comprehensive
    WHERE edu_rank = 1
)

SELECT

    -- =====================================
    -- Snapshot
    -- =====================================
    hc.report_date,

    TO_CHAR(hc.report_date, 'MM/YYYY') AS report_period,

    hc.report_month,
    hc.report_year,
    hc.report_month_no,

    CASE
        WHEN hc.report_date =
            (
                DATE_TRUNC('month', CURRENT_DATE)
                + INTERVAL '1 month - 1 day'
            )::date
        THEN TRUE
        ELSE FALSE
    END AS is_current_snapshot,

    hc.user_status_period AS user_status,
    hc.user_status_period,
    hc.is_user_in,
    hc.is_user_out,
    hc.is_current_user,

    -- =====================================
    -- Employee
    -- =====================================
    hc.member_id,
    hc.member_code,
    hc.member_name,
    hc.member_email,

    hc.member_type,
    hc.gender,
    hc.marital,

    hc.member_level,

    -- =====================================
    -- Organization
    -- =====================================
    hc.branch_name,
    hc.branch_code,

    hc.division_name,
    hc.division_group,

    -- =====================================
    -- Position
    -- =====================================
    hc.position_name,
    hc.position_group,
    hc.group_role_name as position_company_group,

    -- =====================================
    -- Status
    -- =====================================
    hc.member_status,
    hc.active_status,
    hc.member_status_detail,

    -- =====================================
    -- Contract
    -- =====================================
    hc.contract_type,

    -- =====================================
    -- Education
    -- =====================================
    COALESCE(he.school_name, 'Unknown') AS school_name,

    COALESCE(
        NULLIF(he.academic_level, 'N/A'),
        'Unknown'
    ) AS academic_level,

    COALESCE(
        NULLIF(he.degree_name, 'N/A'),
        'Unknown'
    ) AS degree_name,

    COALESCE(
        NULLIF(he.degree_group, 'N/A'),
        'Others'
    ) AS degree_group,

    COALESCE(
        NULLIF(he.graduation_rating, 'N/A'),
        'Unknown'
    ) AS graduation_rating,

    COALESCE(
        NULLIF(he.graduation_rating_group, 'N/A'),
        'Unknown'
    ) AS graduation_rating_group,

    COALESCE(
        he.graduation_year::TEXT,
        'Unknown'
    ) AS graduation_year,

    -- =====================================
    -- Age
    -- =====================================
    hc.age_at_hire,

    hc.birthday,

    EXTRACT(
        YEAR FROM AGE(hc.report_date, hc.birthday)
    )::INT AS current_age,

    -- =====================================
    -- Employment Dates
    -- =====================================
    hc.joining_date,

    hc.start_working_date,

    hc.official_date AS start_date,

    hc.probation_date,
    hc.traineeship_date,

    hc.departure_date,
    hc.resign_date,
    hc.end_date,

    -- =====================================
    -- Seniority
    -- =====================================
    hc.total_months,

    hc.seniority_group,

    CASE
        WHEN hc.total_months < 2 THEN 1
        WHEN hc.total_months < 6 THEN 2
        WHEN hc.total_months < 12 THEN 3
        WHEN hc.total_months < 24 THEN 4
        WHEN hc.total_months < 36 THEN 5
        WHEN hc.total_months < 72 THEN 6
        ELSE 7
    END AS seniority_group_sort,

    CONCAT(
        FLOOR(hc.total_months / 12)::INT,
        ' năm ',
        MOD(hc.total_months, 12),
        ' tháng'
    ) AS seniority_display,
    hc.etl_datetime

FROM {{ ref('dim_hc_snapshot_month') }} hc

LEFT JOIN highest_education he
    ON hc.member_id = he.member_id

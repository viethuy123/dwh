{{ config(materialized='table') }}

WITH contracts AS (

    SELECT
        member_code,
        ROW_NUMBER() OVER (
            PARTITION BY member_code
            ORDER BY date_start DESC
        ) AS rn,
        contract_type
    FROM {{ ref('odoo_hr_contract') }}

),

latest_contracts AS (

    SELECT
        member_code,
        contract_type
    FROM contracts
    WHERE rn = 1

)

SELECT

    -- SCD columns
    e.dbt_scd_id,
    e.dbt_valid_from,
    e.dbt_valid_to,

    CASE
        WHEN e.dbt_valid_to IS NULL
        THEN TRUE
        ELSE FALSE
    END AS is_current,

    -- Member
    e.member_id,
    e.name AS member_name,
    e.work_email AS member_email,
    e.member_code,
    e.member_type,
    e.gender,
    e.marital,

    -- Organization
    e.job_id,

    INITCAP(
        LOWER(
            COALESCE(e.level, 'FRESHER')
        )
    ) AS member_level,

    b.branch_name AS branch_root_name,
    b.branch_code AS branch_root_code,

    b.branch_group_name AS branch_name,
    b.branch_group_code AS branch_code,

    d.division_name,
    d.division_group,

    COALESCE(e.job_title, 'Unknown') AS position_name,

    NULL AS position_group,

    INITCAP(
        LOWER(
            CASE
                WHEN j.group_role_name IS NULL
                     OR LOWER(j.group_role_name) = 'unknown'
                THEN 'Khác'
                ELSE j.group_role_name
            END
        )
    ) AS group_role_name,

    -- Status
    e.state AS member_status,
    e.state_root AS member_status_root,

    lc.contract_type,

    t.member_type_name AS member_status_detail_root,

    INITCAP(
        LOWER(
            CASE
                WHEN t.member_type_name IS NOT NULL
                    THEN t.member_type_name

                WHEN e.start_working_date IS NOT NULL
                    THEN 'chính thức'

                WHEN e.probation_start_date IS NOT NULL
                    THEN 'thử việc'

                WHEN e.traineeship_start_date IS NOT NULL
                    THEN 'thực tập'

                ELSE 'unknown'
            END
        )
    ) AS member_status_detail,

    -- Age
    COALESCE(
        EXTRACT(YEAR FROM e.joining_date)
        - EXTRACT(YEAR FROM e.birthday),
        0
    )::INT AS age_at_hire,

    -- Dates
    e.issue_date_identification,

    e.birthday,

    e.joining_date,

    e.start_working_date,

    COALESCE(
        e.traineeship_start_date,
        e.probation_start_date,
        e.start_working_date,
        e.joining_date,
        e.departure_date,
        e.resign_date,
        CURRENT_DATE
    ) AS official_date,

    e.probation_start_date AS probation_date,

    e.traineeship_start_date AS traineeship_date,

    e.departure_date,

    e.resign_date,

    e.resign_date AS end_date,

    e.etl_datetime

FROM {{ ref('odoo_members_snapshot') }} e

LEFT JOIN {{ ref('odoo_z_type_employee') }} t
    ON CAST(e.type_member_id AS INTEGER)
    = CAST(t.member_type_id AS INTEGER)

LEFT JOIN latest_contracts lc
    ON CAST(e.member_code AS INTEGER)
    = CAST(lc.member_code AS INTEGER)

LEFT JOIN {{ ref('dim_odoo_branch') }} b
    ON e.company_id = b.id

LEFT JOIN {{ ref('dim_odoo_division') }} d
    ON e.division_id = d.id

LEFT JOIN {{ ref('dim_odoo_job') }} j
    ON e.job_id = j.job_id
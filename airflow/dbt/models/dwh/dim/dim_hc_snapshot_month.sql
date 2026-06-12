{{ config(materialized='table') }}

WITH date_bounds AS (

    SELECT
        DATE '2020-01-01' AS start_date,
        CURRENT_DATE AS end_date

),

date_series AS (

    SELECT
        (
            DATE_TRUNC('month', d)
            + INTERVAL '1 month - 1 day'
        )::date AS report_date

    FROM (
        SELECT generate_series(
            (SELECT start_date FROM date_bounds),
            (SELECT end_date FROM date_bounds),
            INTERVAL '1 month'
        ) AS d
    ) s

),

base_data AS (

    SELECT *

    FROM {{ ref('dim_odoo_members') }}

    WHERE member_code IS NOT NULL

),

transfers as (
    select 
        member_id,
        transfer_type_id,
        transfer_start_date,
        transfer_end_date
    from {{ ref('odoo_employee_transfer') }}
    where transfer_type_id in (1,2,3)
),

snapshot_data AS (

    SELECT

        ds.report_date,
        DATE_TRUNC('month', ds.report_date)::date AS report_month,

        EXTRACT(YEAR FROM ds.report_date)::INT AS report_year,
        EXTRACT(MONTH FROM ds.report_date)::INT AS report_month_no,

        CONCAT(
            b.member_code,
            '_',
            TO_CHAR(ds.report_date, 'YYYYMMDD')
        ) AS snapshot_key,

        b.member_id,
        b.member_code,
        b.member_name,
        b.member_email,

        b.member_type,
        b.gender,
        b.marital,

        b.member_level,

        b.branch_root_name,
        b.branch_root_code,

        b.branch_name,
        b.branch_code,

        b.division_name,
        b.division_group,

        b.position_name,
        b.position_group,
        b.group_role_name,

        b.member_status,
        b.member_status_root,

        b.member_status_detail,
        b.member_status_detail_root,
        b.type_member_id,

        b.contract_type,

        b.age_at_hire,

        b.issue_date_identification,
        b.birthday,
        b.start_working_date,
        b.joining_date,
        b.official_date,

        b.probation_date,
        b.traineeship_date,

        b.departure_date,
        b.resign_date,
        b.end_date,
        CASE
            WHEN end_date IS NOT NULL
                AND DATE_TRUNC('month', end_date)
                    = DATE_TRUNC('month', report_date)
            THEN 'Inactive'
            ELSE 'Active'
        END AS active_status,    

        (
            EXTRACT(
                YEAR FROM age(
                    LEAST(
                        COALESCE(b.end_date, ds.report_date),
                        ds.report_date
                    ),
                    b.official_date
                )
            ) * 12
            +
            EXTRACT(
                MONTH FROM age(
                    LEAST(
                        COALESCE(b.end_date, ds.report_date),
                        ds.report_date
                    ),
                    b.official_date
                )
            )
        )::INT AS total_months,
        t.transfer_type_id,
        t.transfer_start_date,
        t.transfer_end_date,

        b.etl_datetime

    FROM date_series ds

    INNER JOIN base_data b
        ON b.official_date <= ds.report_date
       AND (
            b.end_date IS NULL
            OR b.end_date >= date_trunc('month', ds.report_date)
       )

    LEFT JOIN transfers t
        ON b.member_id = t.member_id
       AND 
            ds.report_date >= t.transfer_start_date
       AND 
            ds.report_date <= t.transfer_end_date


)

SELECT

    report_date,
    report_month,
    report_year,
    report_month_no,

    snapshot_key,

    member_id,
    member_code,
    member_name,
    member_email,

    member_type,
    gender,
    marital,

    member_level,
    type_member_id,

    branch_root_name,
    branch_root_code,

    branch_name,
    branch_code,

    division_name,
    division_group,

    position_name,
    position_group,
    group_role_name,

    member_status,
    active_status,
    member_status_root,
    INITCAP(
            LOWER(
        CASE

            -- Transfer ưu tiên cao nhất
            WHEN type_member_id in (7,8) 
            THEN member_status_detail
            WHEN transfer_type_id = 1
            THEN 'Nghỉ thai sản'

            WHEN transfer_type_id = 2
            THEN 'Nghỉ không lương'

            WHEN transfer_type_id = 3
            THEN 'Onsite'

            -- Thực tập

            WHEN traineeship_date IS NOT NULL
                AND report_date >= traineeship_date
                AND (
                        probation_date IS NULL
                        OR report_date < probation_date
                    )
                AND (
                        joining_date IS NULL
                        OR report_date < joining_date
                    )
            THEN 'Thực tập'

            -- Thử việc

            WHEN probation_date IS NOT NULL
                AND report_date >= probation_date
                AND (
                        joining_date IS NULL
                        OR report_date < joining_date
                    )
            THEN 'Thử việc'

            -- Chính thức

            WHEN joining_date IS NOT NULL
                AND report_date >= joining_date
            THEN 'Chính thức'

            -- Fallback

            ELSE member_status_detail

        END 
            ))
    AS member_status_detail,

    member_status_detail_root,

    contract_type,

    age_at_hire,

    issue_date_identification,
    birthday,

    joining_date,
    official_date,
    start_working_date,
    probation_date,
    traineeship_date,

    departure_date,
    resign_date,
    end_date,

    total_months,

    CASE
        WHEN total_months < 2  THEN '1. < 2 tháng'
        WHEN total_months < 6  THEN '2. 2 – < 6 tháng'
        WHEN total_months < 12 THEN '3. 6 – < 12 tháng'
        WHEN total_months < 24 THEN '4. 1 – < 2 năm'
        WHEN total_months < 36 THEN '5. 2 – < 3 năm'
        WHEN total_months < 72 THEN '6. 3 – < 6 năm'
        ELSE '7. >= 6 năm'
    END AS seniority_group,
    transfer_type_id,
    transfer_start_date,
    transfer_end_date,

    etl_datetime

FROM snapshot_data

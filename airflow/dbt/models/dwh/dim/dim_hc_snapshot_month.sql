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
        b.group_role_id,
        b.country_name,
        b.job_id,
        b.member_status,
        b.member_status_root,

        b.member_status_detail,
        -- b.member_status_detail_root,
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
            WHEN b.official_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.official_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
             AND b.end_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.end_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 'in_and_out'
            WHEN b.official_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.official_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 'in'
            WHEN b.end_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.end_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 'out'
            ELSE 'current'
        END AS user_status_period,
        CASE
            WHEN b.official_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.official_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 1
            ELSE 0
        END AS is_user_in,
        CASE
            WHEN b.end_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.end_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 1
            ELSE 0
        END AS is_user_out,
        CASE
            WHEN b.official_date <= ds.report_date
             AND (b.end_date IS NULL OR b.end_date > ds.report_date)
            THEN 1
            ELSE 0
        END AS is_current_user,
        CASE
            WHEN b.end_date IS NOT NULL
             AND DATE_TRUNC('month', b.end_date) = DATE_TRUNC('month', ds.report_date)
            THEN 'Inactive'
            WHEN b.official_date IS NOT NULL
             AND b.official_date >= DATE_TRUNC('month', ds.report_date)::date
             AND b.official_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
            THEN 'Active'
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
        ON (
                (
                    b.official_date <= ds.report_date
                    AND (b.end_date IS NULL OR b.end_date > ds.report_date)
                )
                OR (
                    b.official_date >= DATE_TRUNC('month', ds.report_date)::date
                    AND b.official_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
                )
                OR (
                    b.end_date >= DATE_TRUNC('month', ds.report_date)::date
                    AND b.end_date < (DATE_TRUNC('month', ds.report_date) + INTERVAL '1 month')::date
                )
        )

    LEFT JOIN LATERAL (
        SELECT
            t.transfer_type_id,
            t.transfer_start_date,
            t.transfer_end_date
        FROM transfers t
        WHERE t.member_id = b.member_id
          AND ds.report_date >= t.transfer_start_date
          AND ds.report_date <= t.transfer_end_date
        ORDER BY t.transfer_start_date DESC
        LIMIT 1
    ) t ON TRUE


)
,
_final as (

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
        country_name,
        job_id,
        position_name,
        position_group,
        group_role_name,
        group_role_id,

        member_status,
        active_status,
        user_status_period,
        is_user_in,
        is_user_out,
        is_current_user,
        member_status_root,

        CASE

            -- Transfer ưu tiên cao nhất
            WHEN type_member_id in (7,8,9) 
            THEN type_member_id
            WHEN transfer_type_id = 1
            THEN 12

            WHEN transfer_type_id = 2
            THEN 11

            WHEN transfer_type_id = 3
            THEN 9

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
            THEN 6

            -- Thử việc

            WHEN probation_date IS NOT NULL
                AND report_date >= probation_date
                AND (
                        joining_date IS NULL
                        OR report_date < joining_date
                    )
            THEN 5

            -- Chính thức

            WHEN joining_date IS NOT NULL
                AND report_date >= joining_date
            THEN 4

            -- Fallback

            ELSE type_member_id

        END 

        AS member_status_detail_no,

        member_status_detail,

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
)

Select f.*,
    cm.work_standard as month_standard,
    CASE 
        WHEN f.group_role_id = 4080 THEN 'Thành viên HĐQT'
        WHEN member_status_detail_no = 9 THEN 'Nhân viên phái cử'
        WHEN member_status_detail_no in (7,6) THEN 'Nhân viên part-time'
        ELSE 'Nhân viên chính thức'
    END AS type_hire_name,

    coalesce(dms.member_status_detail,f.member_status_detail, 'Unknown')   AS member_status_detail

from _final f
left join {{ ref('dim_member_status') }} dms
    on f.member_status_detail_no = dms.type_member_id
LEFT JOIN {{ ref('dim_closing_month') }} cm
    ON cm.report_month = f.report_month

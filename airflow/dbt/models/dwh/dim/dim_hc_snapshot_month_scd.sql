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

    FROM {{ ref('dim_odoo_members_scd') }}

    WHERE member_code IS NOT NULL

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

        b.dbt_scd_id,
        b.dbt_valid_from,
        b.dbt_valid_to,
        b.is_current,

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

        b.etl_datetime

    FROM date_series ds

    INNER JOIN base_data b
        ON b.official_date <= ds.report_date
       AND (
            b.end_date IS NULL
            OR b.end_date > ds.report_date
       )
       AND b.dbt_valid_from::date <= ds.report_date
       AND (
            b.dbt_valid_to IS NULL
            OR b.dbt_valid_to::date > ds.report_date
       )

)

SELECT

    report_date,
    report_month,
    report_year,
    report_month_no,

    snapshot_key,

    dbt_scd_id,
    dbt_valid_from,
    dbt_valid_to,
    is_current,

    member_id,
    member_code,
    member_name,
    member_email,

    member_type,
    gender,
    marital,

    member_level,

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
    member_status_root,

    member_status_detail,
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

    etl_datetime

FROM snapshot_data

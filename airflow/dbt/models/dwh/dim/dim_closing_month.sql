{{ config(materialized='table') }}

SELECT
    closing_month_id,
    closing_month_status,
    status,
    order_no,
    is_deleted,
    closing_month_name,
    closing_month_code,
    to_date(closing_month_code, 'MMYYYY') AS report_month,
    created_time,
    created_by,
    end_time,
    start_time,
    updated_time,
    updated_by,
    work_standard,
    note,
    etl_datetime
FROM {{ ref('create_closing_months') }}

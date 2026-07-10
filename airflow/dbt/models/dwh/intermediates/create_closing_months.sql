{{ config(materialized='table') }}

SELECT
    _id AS closing_month_id,
    "closingMonthStatus" AS closing_month_status,
    status,
    "order"::INTEGER AS order_no,
    "isDeleted"::BOOLEAN AS is_deleted,
    "closingMonthName" AS closing_month_name,
    "closingMonthCode" AS closing_month_code,
    {{ safe_parse_timestamp('"createdAt"') }} AS created_time,
    "createdBy" AS created_by,
    {{ safe_parse_timestamp('"endAt"') }} AS end_time,
    {{ safe_parse_timestamp('"startAt"') }} AS start_time,
    {{ safe_parse_timestamp('"updatedAt"') }} AS updated_time,
    "updatedBy" AS updated_by,
    "workStandard"::NUMERIC AS work_standard,
    note,
    etl_datetime
FROM {{ source('create', 'stg_create_closing_months') }}
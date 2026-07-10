{{ config(materialized='table') }}

SELECT
    _id AS member_overtime_id,
    "staffOvertimeDetails" AS member_overtime_details,
    "totalMember"::INTEGER AS total_member,
    "totalHour"::NUMERIC AS total_hour,
    {{ safe_parse_timestamp('"fromDate"') }} AS from_date,
    {{ safe_parse_timestamp('"endDate"') }} AS end_date,
    comment,
    status,
    "order"::INTEGER AS order_no,
    "isDeleted"::BOOLEAN AS is_deleted,
    "projectObjId" AS project_id,
    "reportObjId" AS report_id,
    "createdBy" AS created_by,
    {{ safe_parse_timestamp('"createdAt"') }} AS created_time,
    "updatedBy" AS updated_by,
    {{ safe_parse_timestamp('"updatedAt"') }} AS updated_time,
    "isLogCompensation"::BOOLEAN AS is_log_compensation,
    "isBill"::BOOLEAN AS is_bill,
    "totalHourPrevious"::NUMERIC AS total_hour_previous,
    "isClose"::BOOLEAN AS is_close,
    "hrAccount" AS hr_account,
    {{ safe_parse_timestamp('"hrImportedAt"') }} AS hr_imported_time,
    branch,
    "userObjId" AS member_id,
    etl_datetime
FROM {{ source('create', 'stg_create_staff_overtimes') }}
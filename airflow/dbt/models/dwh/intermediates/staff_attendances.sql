{{ config(materialized='table') }}

SELECT
    _id as attendance_id,
    "userObjId" as user_id,
    "attendanceTypeObjId" as attendance_type_id,
    {{ safe_parse_timestamp('"fromDate"') }} as from_date,
    {{ safe_parse_timestamp('"endDate"') }} as end_date,
    "absentDay" as absent_day,
    reason as absent_reason,
    "statusApproval" as status_approval,
    {{ safe_parse_timestamp('"dateApproval"') }} as date_approval,
    type,
    status,
    "isDeleted" as is_deleted,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_staff_attendances') }}
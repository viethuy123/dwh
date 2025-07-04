{{ config(materialized='table') }}

SELECT
    _id as attendance_id,
    "userObjId" as user_id,
    "attendanceTypeObjId" as attendance_type_id,
    to_timestamp("fromDate", 'YYYY-MM-DD HH24:MI:SS') as from_date,
    to_timestamp("endDate", 'YYYY-MM-DD HH24:MI:SS') as end_date,
    "absentDay" as absent_day,
    reason as absent_reason,
    "statusApproval" as status_approval,
    to_timestamp("dateApproval", 'YYYY-MM-DD HH24:MI:SS') as date_approval,
    type,
    status,
    "isDeleted" as is_deleted,
    TO_TIMESTAMP("createdAt",'YYYY-MM-DD HH24:MI:SS') as created_time,
    TO_TIMESTAMP("updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_staff_attendances') }}
{{ config(materialized='table') }}

SELECT
    sa.attendance_id,
    sa.user_id,
    u.user_name,
    u.company_email,
    u.staff_code,
    sa.attendance_type_id,
    sat.attendance_type_name,
    sat.attendance_type_code,
    sat.attendance_work_day,
    sat.unit_status,
    sat.unit_type,
    sat.unit_value,
    sa.from_date,
    sa.end_date,
    sa.absent_day, 
    sa.absent_reason,
    sa.status_approval,
    sa.date_approval,
    sa.type as attendance_record_type,
    sa.status as attendance_status,
    sa.is_deleted as attendance_is_deleted,
    sa.created_time,
    sa.updated_time,
    sa.etl_datetime
FROM {{ ref('staff_attendances') }} sa
LEFT JOIN {{ ref('staff_attendances_types') }} sat
    ON sa.attendance_type_id = sat.attendance_type_id
LEFT JOIN {{ ref('users') }} u
    ON sa.user_id = u.user_id

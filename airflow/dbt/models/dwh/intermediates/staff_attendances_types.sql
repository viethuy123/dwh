{{ config(materialized='table') }}

SELECT
    _id as attendance_type_id,
    "attendanceTypeName" as attendance_type_name,
    "attendanceTypeCode" as attendance_type_code,
    "attendanceWorkDay" as attendance_work_day,
    status,
    "isDeleted" as is_deleted,
    type,
    "unitStatus" as unit_status,
    "unitType" as unit_type,
    "unitValue" as unit_value,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_staff_attendance_types') }}
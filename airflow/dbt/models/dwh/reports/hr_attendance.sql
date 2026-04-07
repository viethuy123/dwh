{{ config(materialized='table') }}

WITH attendance_daily AS (
    SELECT * FROM {{ ref('member_attendance_daily') }}
),

dim_type AS (
    SELECT * FROM {{ ref('dim_attendance_types') }}
),

dim_employee AS (
    SELECT * FROM {{ ref('dim_odoo_members') }}
)

SELECT
    -- 1. Định danh nhân viên (Gốc để chứa tất cả User)
    e.member_id,
    e.member_name,
    e.position_name,
    e.branch_name,
    e.branch_code,
    e.division_name,
    e.division_group,
    e.member_status,



    -- 2. Thông tin ngày nghỉ (NULL nếu ngày đó không nghỉ)
    ad.date_actual,
    ad.daily_absent_unit,
    -- ad.month_actual,
    -- ad.year_actual,

    -- 3. Thông tin loại nghỉ (Từ bảng Type)
    dt.attendance_type_name,
    dt.attendance_type_code,
    dt.type as category_type,

    -- 4. Thông tin đơn gốc (Để trace ngược lại)
    ad.attendance_id,
    ad.absent_reason,
    ad.original_from_date,
    ad.original_end_date,
    ad.total_absent_days_original,
    e.etl_datetime

FROM dim_employee e
LEFT JOIN attendance_daily ad ON e.member_code = ad.member_code
LEFT JOIN dim_type dt ON ad.attendance_type_id = dt.attendance_type_id
{{ config(
    materialized='table'
) }}

with user_data as (
    select 
        *
    from {{ ref('dim_members_new') }}
),

seniority_calc AS (
    SELECT 
        *,
        -- Bước 1: Xác định ngày mốc để tính (Ngày nghỉ hoặc Ngày hiện tại)
        COALESCE(end_date, CURRENT_DATE) as reference_date
    FROM user_data
),

diff_parts AS (
    SELECT 
        *,
        -- Bước 2: Dùng hàm AGE để lấy khoảng cách thời gian chi tiết
        AGE(reference_date, official_date) as age_interval,
        EXTRACT(year FROM AGE(reference_date, official_date)) as yrs,
        EXTRACT(month FROM AGE(reference_date, official_date)) as mons,
        -- Tính tổng số tháng để phân nhóm chính xác
        (EXTRACT(year FROM AGE(reference_date, official_date)) * 12 + 
         EXTRACT(month FROM AGE(reference_date, official_date))) as total_months
    FROM seniority_calc
),

final_transformation as (
    SELECT 
        *,
        -- Bước 3: Build chuỗi hiển thị thâm niên
        TRIM(
            CASE WHEN yrs > 0 THEN yrs || ' năm ' ELSE '' END ||
            CASE WHEN mons > 0 THEN mons || ' tháng' ELSE '' END ||
            CASE WHEN yrs = 0 AND mons = 0 THEN 'Dưới 1 tháng' ELSE '' END
        ) as seniority_display,

        -- Bước 4: Chia nhóm thâm niên
        CASE 
            WHEN total_months < 12 THEN '< 12 tháng'
            WHEN total_months < 24 THEN '1–2 năm'
            WHEN total_months < 36 THEN '2–3 năm'
            WHEN total_months < 72 THEN '3–6 năm'
            ELSE '> 6 năm'
        END as seniority_group,

        -- Bước 5: Tạo cột sắp xếp (Quan trọng để lên biểu đồ đúng thứ tự)
        CASE 
            WHEN total_months < 12 THEN 1
            WHEN total_months < 24 THEN 2
            WHEN total_months < 36 THEN 3
            WHEN total_months < 72 THEN 4
            ELSE 5
        END as seniority_group_sort
    FROM diff_parts
)

SELECT 
    member_id,
    member_name,
    member_email,
    staff_code,
    branch_name,
    branch_code,
    department_name,
    position_name,
    user_level,
    user_status,
    create_date,
    official_date,
    birth_day,
    age,
    create_date_used,
    end_date,
    count_email_duplicates,
    etl_datetime,
    reference_date,
    
    -- Ép kiểu sang TEXT để tránh lỗi DQ "float() argument ... not Timedelta"
    age_interval::TEXT as age_interval, 
    
    yrs,
    mons,
    total_months,
    seniority_display,
    seniority_group,
    seniority_group_sort
FROM final_transformation
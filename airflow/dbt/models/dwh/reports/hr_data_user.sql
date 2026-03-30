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

education_comprehensive AS (
    SELECT 
        e.employee_code,
        s.school_name,
        al.level_name AS academic_level_name,
        q.qualification_name,
        gr.graduation_rank_name,
        ed.faculty AS major,
        ed.graduation_year,
        -- Sắp xếp để lấy bằng cấp cao nhất của 1 user
        ROW_NUMBER() OVER (
            PARTITION BY e.employee_code 
            ORDER BY al.sequence_order DESC, ed.graduation_year DESC
        ) as edu_rank
    FROM {{ ref('odoo_hr_employee_education') }} ed
    JOIN {{ ref('odoo_hr_employee') }} e ON ed.employee_id = e.employee_id
    LEFT JOIN {{ ref('odoo_hr_employee_school') }} s 
        ON ed.study_school_id = s.school_id
    LEFT JOIN {{ ref('odoo_z_academic_level') }} al 
        ON ed.academic_level_id = al.academic_level_id
    LEFT JOIN {{ ref('odoo_z_qualification') }} q 
        ON ed.qualification_id = q.qualification_id
    LEFT JOIN {{ ref('odoo_hr_graduation_rank') }} gr 
        ON ed.rank_id = gr.graduation_rank_id
),

highest_education AS (
    SELECT * FROM education_comprehensive WHERE edu_rank = 1
),

final_transformation as (
    SELECT 
        dp.*,
        he.school_name,
        he.academic_level_name,
        he.qualification_name,
        he.graduation_rank_name,
        he.major,
        he.graduation_year,
        -- Bước 3: Build chuỗi hiển thị thâm niên
        TRIM(
            CASE WHEN yrs > 0 THEN yrs || ' năm ' ELSE '' END ||
            CASE WHEN mons > 0 THEN mons || ' tháng' ELSE '' END ||
            CASE WHEN yrs = 0 AND mons = 0 THEN 'Dưới 1 tháng' ELSE '' END
        ) as seniority_display,

        -- Bước 4: Chia nhóm thâm niên
        CASE 
            WHEN total_months < 2 THEN '< 2 tháng'
            WHEN total_months < 6 THEN '2 – < 6 tháng'
            WHEN total_months < 12 THEN '6 – < 12 tháng'
            WHEN total_months < 24 THEN '1 – < 2 năm'
            WHEN total_months < 36 THEN '2 – < 3 năm'
            WHEN total_months < 72 THEN '3 – < 6 năm'
            ELSE '>= 6 năm'
        END as seniority_group,

        -- Bước 5: Tạo cột sắp xếp (Quan trọng để lên biểu đồ đúng thứ tự)
        CASE 
            WHEN total_months < 2 THEN 1
            WHEN total_months < 6 THEN 2
            WHEN total_months < 12 THEN 3
            WHEN total_months < 24 THEN 4
            WHEN total_months < 36 THEN 5
            WHEN total_months < 72 THEN 6
            ELSE 7
        END as seniority_group_sort
    FROM diff_parts dp
    LEFT JOIN highest_education he 
    ON dp.staff_code = he.employee_code
)

SELECT 
    member_id,
    member_name,
    member_email,
    staff_code,
    branch_name,
    branch_code,
    division_name,
    division_group,
    COALESCE(school_name, 'unknown') as school_name,
    COALESCE(NULLIF(academic_level_name, 'N/A'), 'unknown') as academic_level_name,
    COALESCE(NULLIF(qualification_name, 'N/A'), 'unknown') as qualification_name,
    COALESCE(NULLIF(graduation_rank_name, 'N/A'), 'unknown') as graduation_rank_name,
    COALESCE(NULLIF(major, 'N/A'), 'unknown') as major,
    COALESCE(NULLIF(graduation_year, 'N/A'), 'unknown') as graduation_year,
    position_name,
    user_level,
    user_status,
    position_group,
    create_date,
    official_date,
    birth_day,
    age_at_hire as age,
    create_date_used,
    end_date,
    count_email_duplicates,
    etl_datetime,
    reference_date,
    extract(year from age(birth_day)) AS current_age,
    -- Ép kiểu sang TEXT để tránh lỗi DQ "float() argument ... not Timedelta"
    age_interval::TEXT as age_interval, 
    
    yrs,
    mons,
    total_months,
    seniority_display,
    seniority_group,
    seniority_group_sort
FROM final_transformation
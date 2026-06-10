{{ config(
    materialized='table'
) }}

with user_data as (
    select 
        *
    from {{ ref('dim_odoo_members') }}
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
        member_id,
        school_name,
        academic_level,
        degree_name,
        degree_group,
        graduation_rating,
        graduation_rating_group,
        graduation_year,

        -- Sắp xếp để lấy bằng cấp cao nhất của 1 user
        ROW_NUMBER() OVER (
            PARTITION BY member_id 
            ORDER BY record_created_at DESC, graduation_year DESC
        ) as edu_rank
    FROM {{ ref('fct_member_education') }} 
    
),

highest_education AS (
    SELECT * FROM education_comprehensive WHERE edu_rank = 1
),

final_transformation as (
    SELECT 
        dp.*,
        he.school_name,
        he.academic_level,
        he.degree_name,
        he.degree_group,
        he.graduation_rating,
        he.graduation_rating_group,
        he.graduation_year,
        -- Bước 3: Build chuỗi hiển thị thâm niên
        TRIM(
            CASE WHEN yrs > 0 THEN yrs || ' năm ' ELSE '' END ||
            CASE WHEN mons > 0 THEN mons || ' tháng' ELSE '' END ||
            CASE WHEN yrs = 0 AND mons = 0 THEN 'Dưới 1 tháng' ELSE '' END
        ) as seniority_display,

        -- Bước 4: Chia nhóm thâm niên
        CASE 
            WHEN total_months < 2 THEN  '1.  < 2 tháng'
            WHEN total_months < 6 THEN  '2.  6 – 12 tháng'
            WHEN total_months < 12 THEN '3.  < 1 năm'
            WHEN total_months < 24 THEN '4.  1 – < 2 năm'
            WHEN total_months < 36 THEN '5.  2 – < 3 năm'
            WHEN total_months < 72 THEN '6.  3 – < 6 năm'
            ELSE '7.  >= 6 năm'
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
    ON dp.member_id = he.member_id
),

full_date as (
    SELECT 
        d.date_actual,
        d.day_name,
        d.day_of_week,
        d.is_weekend,
        ft.*
    FROM {{ ref('dim_date') }} d
    left join final_transformation ft
    on d.date_actual = ft.official_date
)

SELECT 
    date_actual,
    day_name,
    day_of_week,
    is_weekend,
    member_id,
    member_name,
    member_email,
    member_code,
    branch_name,
    branch_code,
    division_name,
    division_group,
    COALESCE(school_name, 'Unknown') as school_name,
    COALESCE(NULLIF(academic_level, 'N/A'), 'Unknown') as academic_level,
    COALESCE(NULLIF(degree_name, 'N/A'), 'Unknown') as degree_name,
    COALESCE(NULLIF(degree_group, 'N/A'), 'Others') as degree_group,
    COALESCE(NULLIF(graduation_rating, 'N/A'), 'Unknown') as graduation_rating,
    COALESCE(NULLIF(graduation_rating_group, 'N/A'), 'Unknown') as graduation_rating_group,
    COALESCE(NULLIF(graduation_year, 'N/A'), 'Unknown') as graduation_year,
    position_name,
    position_group,
    group_role_name as position_company_group,
    member_level,
    member_status,
    gender,
    marital,
    -- user_status_originals,
    INITCAP(
        LOWER(
            CASE
                WHEN LOWER(member_status_detail) = 'thôi việc'
                    THEN 'chính thức'
                ELSE member_status_detail
            END
        )
    ) as user_status_originals_detail,
    -- member_status_detail as user_status_originals_detail,
    age_at_hire as age,
    extract(year from age(birthday)) AS current_age,

    -- create_date,
    start_working_date,
    official_date,
    probation_date,
    traineeship_date,
    birthday,
    -- create_date_used,
    end_date,
    etl_datetime,
    
    -- Ép kiểu sang TEXT để tránh lỗi DQ "float() argument ... not Timedelta"
    age_interval::TEXT as age_interval, 
    
    yrs,
    mons,
    total_months,
    seniority_display,
    seniority_group,
    seniority_group_sort
FROM full_date
-- where member_code is not null
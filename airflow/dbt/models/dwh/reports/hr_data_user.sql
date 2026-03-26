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
            WHEN total_months < 12 THEN '< 1 năm'
            WHEN total_months < 24 THEN '1 – < 2 năm'
            WHEN total_months < 36 THEN '2 – < 3 năm'
            WHEN total_months < 72 THEN '3 – < 6 năm'
            ELSE '>= 6 năm'
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
    COALESCE(NULLIF(branch_name, 'NO'), 'Unknown') AS branch_name,
    COALESCE(NULLIF(branch_code, 'NO'), 'Unknown') AS branch_code,
    COALESCE(NULLIF(department_name, 'NO'), 'Unknown') AS department_name,
    COALESCE(NULLIF(position_name, 'NO'), 'Unknown') AS position_name,
    COALESCE(NULLIF(user_level, 'NO'), 'Unknown') AS user_level,
    COALESCE(NULLIF(user_status, 'NO'), 'Unknown') AS user_status,
    CASE 
        -- INTERN / TRAINEE
        WHEN position_name ILIKE '%intern%' 
        OR position_name ILIKE '%thử việc%' 
        OR position_name ILIKE '%học việc%' 
        OR position_name ILIKE '%fresher%' 
        THEN 'INTERN_TRAINEE'

        -- MANAGEMENT
        WHEN position_name ILIKE '%manager%' 
        OR position_name ILIKE '%director%' 
        OR position_name ILIKE '%head%' 
        OR position_name ILIKE '%leader%' 
        OR position_name ILIKE '%ceo%' 
        OR position_name ILIKE '%cto%' 
        THEN 'MANAGEMENT'

        -- ENGINEERING
        WHEN position_name ILIKE '%developer%' 
        OR position_name ILIKE '%engineer%' 
        OR position_name ILIKE '%data%' 
        OR position_name ILIKE '%ai%' 
        OR position_name ILIKE '%machine learning%' 
        OR position_name ILIKE '%tester%' 
        OR position_name ILIKE '%qa%' 
        OR position_name ILIKE '%devops%' 
        OR position_name ILIKE '%infra%' 
        OR position_name ILIKE '%cloud%' 
        THEN 'ENGINEERING'

        -- PRODUCT / BA
        WHEN position_name ILIKE '%ba%' 
        OR position_name ILIKE '%business analyst%' 
        OR position_name ILIKE '%product%' 
        THEN 'PRODUCT_BA'

        -- SALES
        WHEN position_name ILIKE '%sale%' 
        OR position_name ILIKE '%account%' 
        OR position_name ILIKE '%business development%' 
        OR position_name ILIKE '%pre-sales%' 
        THEN 'SALES'

        -- MARKETING
        WHEN position_name ILIKE '%marketing%' 
        OR position_name ILIKE '%mkt%' 
        OR position_name ILIKE '%content%' 
        OR position_name ILIKE '%seo%' 
        THEN 'MARKETING'

        -- HR / ADMIN
        WHEN position_name ILIKE '%hr%' 
        OR position_name ILIKE '%admin%' 
        OR position_name ILIKE '%accountant%' 
        OR position_name ILIKE '%ta%' 
        OR position_name ILIKE '%ga%' 
        THEN 'HR_ADMIN'

        -- OPERATION
        WHEN position_name ILIKE '%project%' 
        OR position_name ILIKE '%delivery%' 
        OR position_name ILIKE '%operation%' 
        OR position_name ILIKE '%support%' 
        THEN 'OPERATION'

        ELSE 'OTHER'
        END as position_group,
    
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
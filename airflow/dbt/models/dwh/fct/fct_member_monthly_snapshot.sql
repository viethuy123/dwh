{{ config(materialized='table') }}
WITH last_days AS (
    SELECT date_actual AS date
    FROM {{ ref('dim_date') }}
    WHERE date_actual = (date_trunc('month', date_actual) + interval '1 month - 1 day')::date
),
dim_member AS (
    SELECT 
        member_id,
        member_email,
        member_name,
        official_date,
        create_date_used as start_date, 
        end_date,
        end_date as exp_date,     
        create_date_used as eff_date,            
        user_status,
        position_name as standard_role,
        position_name as group_role
        -- Giả sử sau này bạn thêm cột branch ở đây
        -- branch_name, 
        -- is_billable
    FROM {{ ref('dim_members_new') }}
)
SELECT 
    ld.date AS month_key,
    m.member_id,
    m.user_status,
    m.standard_role,
    m.group_role,
    -- m.branch_name,
    m.official_date,
    m.end_date,
    -- Luôn tính thâm niên từ ngày vào làm gốc (official_date)
    (EXTRACT(YEAR FROM age(ld.date, m.official_date)) * 12 + 
     EXTRACT(MONTH FROM age(ld.date, m.official_date))) AS tenure_months
FROM last_days ld
JOIN dim_member m 
  -- ĐIỀU KIỆN 1: Dòng dữ liệu (Role/Branch) phải đang có hiệu lực
  ON m.eff_date <= ld.date 
  AND (m.exp_date IS NULL OR m.exp_date > ld.date)
  -- ĐIỀU KIỆN 2: Nhân viên phải đang trong trạng thái làm việc (Chưa nghỉ hẳn)
  AND m.official_date <= ld.date
  AND (m.end_date IS NULL OR m.end_date > ld.date)
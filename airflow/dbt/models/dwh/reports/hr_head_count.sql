config(materialized='table')
WITH monthly_hc AS (
    -- Bước 1: Gom nhóm sơ bộ theo từng tháng và các thuộc tính
    SELECT 
        month_key,
        user_status,
        standard_role,
        group_role,
        -- Giả sử sau này bạn thêm branch_name ở đây
        -- branch_name,
        COUNT(member_id) AS hc_count
    FROM fact_member_monthly_snapshot
    GROUP BY 1, 2, 3, 4
),
calculated_delta AS (
    -- Bước 2: Dùng Window Function để lấy số lượng của tháng trước (Kỳ trước)
    SELECT 
        *,
        LAG(hc_count) OVER (
            PARTITION BY user_status, standard_role, group_role 
            ORDER BY month_key
        ) AS hc_previous_month
    FROM monthly_hc
)
-- Bước 3: Đưa ra kết quả cuối cùng với các cột phân loại
SELECT 
    month_key,
    -- Thuộc tính gốc
    user_status AS loai_hinh,       -- Phục vụ BC 01
    standard_role,                  -- Phục vụ BC 04
    group_role,                     -- Phục vụ BC 05
    hc_count AS hc_current,
    
    -- Tính Delta (Chênh lệch) cho BC 04, 05
    COALESCE(hc_previous_month, 0) AS hc_previous,
    (hc_count - COALESCE(hc_previous_month, 0)) AS delta_abs,
    
    -- Tính % Tăng trưởng cho BC 02
    CASE 
        WHEN COALESCE(hc_previous_month, 0) = 0 THEN NULL 
        ELSE ROUND(((hc_count - hc_previous_month)::numeric / hc_previous_month) * 100, 2)
    END AS growth_percentage,

    -- Các cột hỗ trợ lọc Quý/Năm (Time Intelligence)
    CASE WHEN EXTRACT(MONTH FROM month_key) IN (3, 6, 9, 12) THEN TRUE ELSE FALSE END AS is_quarter_end,
    CASE WHEN EXTRACT(MONTH FROM month_key) = 12 THEN TRUE ELSE FALSE END AS is_year_end,
    
    -- Format hiển thị cho Metabase dễ nhìn
    TO_CHAR(month_key, 'Mon YYYY') AS month_display,
    'Q' || EXTRACT(QUARTER FROM month_key) || '-' || EXTRACT(YEAR FROM month_key) AS quarter_display
FROM calculated_delta;
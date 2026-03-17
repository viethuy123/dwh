{{ config(materialized='table') }}

WITH processed AS (
    SELECT 
        *,
        -- CASE 1: Xử lý email trùng (end_date_1)
        (DATE_TRUNC('month', 
            LEAD(created_at) OVER (PARTITION BY email_company ORDER BY created_at ASC)
        ) - INTERVAL '1 day')::DATE AS email_end_date        
        -- CASE 2: Xử lý nhập liệu sai (Gom ngày nghỉ lớn nhất theo staff_code)
        MAX(quit_date) OVER (PARTITION BY staff_code) AS quit_date_use,
        MAX(official_date) OVER (PARTITION BY staff_code) AS official_date_use,
        MAX(probation_date) OVER (PARTITION BY staff_code) AS probation_date_use,
        MAX(intern_date) OVER (PARTITION BY staff_code) AS intern_date_use,
        -- Đánh số để lọc record mới nhất
        ROW_NUMBER() OVER (PARTITION BY staff_code ORDER BY created_at DESC) AS rn
    FROM {{ ref('users_infos') }}
    WHERE staff_code IS NOT NULL
)
SELECT 
    *,
    -- Áp dụng đúng logic: end_date_2 = COALESCE(quit_date, email_end_date, quit_date_use)
    CASE 
        WHEN COALESCE(quit_date, email_end_date, quit_date_use) > quit_date_use 
        THEN COALESCE(quit_date, quit_date_use, email_end_date)
        ELSE COALESCE(quit_date, email_end_date, quit_date_use) 
    END AS end_date
FROM processed
WHERE rn = 1
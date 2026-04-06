{{ config(materialized='table') }}

WITH attendance AS (
    SELECT 
        *,
        (end_date::date - from_date::date) + 1 AS calendar_days_count
    FROM {{ ref('create_attendances') }}
    WHERE is_deleted = 'No' 
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
    WHERE is_weekend = false 
),

expanded_base AS (
    SELECT
        a.attendance_id,
        a.user_id AS member_id,
        a.attendance_type_id,
        a.absent_reason,
        a.from_date AS original_from_date,
        a.end_date AS original_end_date,
        a.absent_day AS total_absent_days_original,
        d.date_actual,
        a.etl_datetime,
        -- Bước 1: Đánh số thứ tự các ngày làm việc có thể nghỉ
        ROW_NUMBER() OVER (PARTITION BY a.attendance_id ORDER BY d.date_actual) as day_rank,
        -- Bước 2: Đếm xem có tổng cộng bao nhiêu dòng tiềm năng (để biết dòng nào là dòng cuối cùng sau khi filter)
        CEIL(a.absent_day) as max_rank_allowed
    FROM attendance a
    INNER JOIN date_dim d 
        ON d.date_actual BETWEEN a.from_date::date AND a.end_date::date
),

filtered_rows AS (
    -- Bước 3: Cắt đuôi ngay lập tức. Nếu absent = 6, chỉ giữ 6 dòng.
    SELECT * FROM expanded_base
    WHERE day_rank <= max_rank_allowed
)

-- Bước 4: Gán giá trị vào các dòng đã giữ lại
SELECT 
    u.staff_code as member_code,
    attendance_id,
    member_id,
    attendance_type_id,
    absent_reason,
    original_from_date,
    original_end_date,
    total_absent_days_original,
    date_actual,
    f.etl_datetime,
    
    CASE 
        -- Nếu là các ngày trước ngày cuối cùng -> Chắc chắn là 1 công
        WHEN day_rank < max_rank_allowed THEN 1
        
        -- Nếu là ngày cuối cùng (Ví dụ ngày thứ 6 trong đơn 6 ngày, hoặc ngày thứ 7 trong đơn 6.5 ngày)
        ELSE 
            CASE 
                -- Nếu absent_day là số nguyên (6.0, 5.0) -> Ngày cuối nhận 1
                WHEN total_absent_days_original = max_rank_allowed THEN 1
                -- Nếu absent_day lẻ (6.5, 4.25) -> Ngày cuối nhận phần dư (0.5, 0.25)
                ELSE (total_absent_days_original - floor(total_absent_days_original))
            END
    END AS daily_absent_unit

FROM filtered_rows f
JOIN {{ ref('users') }} u ON f.member_id = u.user_id
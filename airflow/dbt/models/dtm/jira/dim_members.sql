{{ config(materialized='table') }}



WITH priority_status AS (
    SELECT 
        u.*,
        CASE 
            WHEN u.user_status IS NOT NULL AND u.user_status != '' 
            AND u.user_status NOT ILIKE '%Inac%' 
            THEN 1
            ELSE 0  -- Inactive/Null/Empty Status
        END AS sort_priority -- Cột ưu tiên

    FROM {{ source('dwh', 'users') }} u
),

get_end_date AS (
    SELECT 
        a.*,
        -- SẮP XẾP: Các bản ghi sort_priority=0 (Inactive) được xếp trước theo thời gian, 
        -- Bản ghi sort_priority=1 (Active) được đẩy xuống cuối.
        LEAD(date(create_time), 1, DATE('2999-12-31')) OVER (
            PARTITION BY company_email
            ORDER BY sort_priority ASC, (create_time) ASC 
        ) AS end_date

    FROM priority_status a

),
cleaned_data AS (
    SELECT
        *
    FROM get_end_date
    WHERE 
        date(create_time) < end_date 
        OR end_date = DATE('2999-12-31')
),

sort_data AS (
    SELECT 
        g.*,
        ROW_NUMBER() OVER (
            PARTITION BY company_email
            ORDER BY end_date ASC 
        ) AS rn
    FROM cleaned_data g
),


cleaned_users as (
    select * ,
    case when rn = 1 then '1999-12-31' else date(create_time) end as create_date_used
    from sort_data
)
SELECT
    a.user_id as member_id,
    a.user_name as member_name,
    a.company_email as member_email,
    a.staff_code,
    b.branch_name,
    b.branch_code,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status,
    date(a.create_time) as create_date,
    a.create_date_used,
    a.end_date
FROM cleaned_users a
LEFT JOIN {{ source('dwh', 'branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ source('dwh', 'departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ source('dwh', 'user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'
-- and a.user_status not IN ('Inactivity', 'null')

GROUP BY
    member_id,
    member_name,
    member_email,
    staff_code,
    b.branch_name,
    b.branch_code,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status,
    date(a.create_time),
    a.create_date_used,
    a.end_date

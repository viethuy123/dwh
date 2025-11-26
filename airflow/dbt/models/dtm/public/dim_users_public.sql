{{ config(materialized='table') }}

with cleaned_users as (
    select * ,
    LEAD(date(create_time), 1, '2999-12-31') OVER (
            PARTITION BY company_email
            ORDER BY date(create_time)
        ) AS end_date
    from {{ source('dwh', 'users') }} a

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
    date(a.update_time) as update_date,
    a.end_date
FROM cleaned_users  a
JOIN {{ source('dwh', 'branches') }} b
ON a.branch_id = b.branch_id
JOIN {{ source('dwh', 'departments') }} c
ON a.department_id = c.department_id
JOIN {{ source('dwh', 'user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'

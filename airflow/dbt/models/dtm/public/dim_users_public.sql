{{ config(materialized='table') }}

with cleaned as (
    select * ,
    LEAD(date(create_time), 1, '2999-12-31') OVER (
            PARTITION BY company_email
            ORDER BY (create_time)
        ) AS end_date,
    ROW_NUMBER() OVER (
            PARTITION BY company_email
            ORDER BY (create_time)
        ) AS rn
    from {{ source('dwh', 'users') }} a

),

cleaned_users as (
    select * ,
    case when rn = 1 then '1999-12-31' else date(create_time) end as create_date_used
    from cleaned c
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
    create_date_used as create_date_used,
    date(a.create_time) as create_date,
    date(a.update_time) as update_date,
    a.end_date
FROM cleaned_users  a
left JOIN {{ source('dwh', 'branches') }} b
ON a.branch_id = b.branch_id
left JOIN {{ source('dwh', 'departments') }} c
ON a.department_id = c.department_id
left JOIN {{ source('dwh', 'user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'

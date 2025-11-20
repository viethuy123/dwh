{{ config(materialized='table') }}


SELECT
    a.user_id as member_id,
    a.user_name as member_name,
    a.company_email as member_email,
    a.staff_code,
    b.branch_name,
    CASE
    WHEN lower(b.branch_name) LIKE '%trụ%' THEN 'HN'
    WHEN lower(b.branch_name) LIKE '%tp%' THEN 'HCM'
    ELSE 'DN'
    END AS branch_code,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status,
    a.create_time
FROM {{ source('dwh', 'users') }} a
LEFT JOIN {{ source('dwh', 'branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ source('dwh', 'departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ source('dwh', 'user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'

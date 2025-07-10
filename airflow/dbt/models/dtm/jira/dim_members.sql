{{ config(materialized='table') }}


SELECT
    a.user_id as member_id,
    a.user_name as member_name,
    a.company_email as member_email,
    a.staff_code,
    b.branch_name,
    c.department_name,
    d.position_name,
    a.user_level,
    a.user_status
FROM {{ source('dwh', 'users') }} a
LEFT JOIN {{ source('dwh', 'branches') }} b
ON a.branch_id = b.branch_id
LEFT JOIN {{ source('dwh', 'departments') }} c
ON a.department_id = c.department_id
LEFT JOIN {{ source('dwh', 'user_positions') }} d
ON a.position_id = d.position_id
WHERE a.company_email is not NULL AND a.company_email != 'null' AND a.company_email NOT LIKE 'Inactive%'
AND b.branch_name is not NULL AND b.branch_name != 'null'
AND c.department_name is not NULL AND c.department_name != 'null'
AND d.position_name is not NULL AND d.position_name != 'null'
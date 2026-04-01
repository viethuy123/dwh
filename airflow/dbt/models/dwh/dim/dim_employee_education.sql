{{ config(materialized='table') }}

WITH education AS (
    SELECT * FROM {{ ref('odoo_hr_employee_education') }}
),

schools AS (
    SELECT * FROM {{ ref('odoo_hr_employee_school') }}
),

academic_levels AS (
    SELECT * FROM {{ ref('odoo_z_academic_level') }}
),

qualifications AS (
    SELECT * FROM {{ ref('odoo_z_qualification') }}
),

grad_ranks AS (
    SELECT * FROM {{ ref('odoo_hr_graduation_rank') }}
)

SELECT
    ed.employee_id,
    -- Thông tin trường học
    s.school_name,
    s.school_type,
    
    -- Trình độ & Bằng cấp
    al.level_name AS academic_level,
    q.qualification_name AS degree_name,
    ed.faculty AS major, -- Chuyên ngành
    ed.graduation_year,
    
    -- Xếp loại tốt nghiệp
    gr.graduation_rank_name AS graduation_rating,
    
    -- Audit info
    ed.create_date AS record_created_at,
    ed.etl_datetime AS setl_datetime
FROM education ed
LEFT JOIN schools s ON ed.study_school_id = s.school_id
LEFT JOIN academic_levels al ON ed.academic_level_id = al.academic_level_id
LEFT JOIN qualifications q ON ed.qualification_id = q.qualification_id
LEFT JOIN grad_ranks gr ON ed.rank_id = gr.graduation_rank_id
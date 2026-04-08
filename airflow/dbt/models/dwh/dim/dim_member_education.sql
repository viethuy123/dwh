{{ config(materialized='table') }}

WITH education AS (
    SELECT * FROM {{ ref('odoo_hr_member_education') }}
),

schools AS (
    SELECT * FROM {{ ref('odoo_hr_member_school') }}
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
    ed.education_id,
    ed.member_id,
    -- Thông tin trường học
    s.school_name,
    s.school_type,
    
    -- Trình độ & Bằng cấp
    al.level_name AS academic_level,
    q.qualification_name AS degree_name,
    CASE
        WHEN LOWER(q.qualification_name) IN (
            'it-web',
            'network',
            'software',
            'computer science and engineering'
        )
            THEN 'Tech - Software'
        WHEN LOWER(q.qualification_name) ~ '(data|ai|machine learning|nlp|phân tích dữ liệu|devops|cloud|network|mạng|security|qa|qc|tester|kiểm thử)'
            THEN 'Tech - Data/Infra/QA'
        WHEN LOWER(q.qualification_name) ~ '(\bit\b|software|web|lập trình|programming|developer|computer science|khoa học máy tính|cntt)'
            THEN 'Tech - Software'
        WHEN LOWER(q.qualification_name) ~ '(nhật|japanese|日本語|ngôn ngữ|language|english|tiếng anh|korean|hàn|trung|chinese)'
            THEN 'Language'
        WHEN LOWER(q.qualification_name) ~ '(marketing|truyền thông|communication|pr|content|design|thiết kế|đồ họa|ui|ux)'
            THEN 'Marketing / Design'
        WHEN LOWER(q.qualification_name) ~ '(kinh tế|economics|business|quản trị|finance|tài chính|ngân hàng|kế toán|accounting|kiểm toán)'
            THEN 'Business / Finance'
        ELSE 'Others'
    END AS degree_group,
    ed.faculty AS major, -- Chuyên ngành
    ed.graduation_year,
    
    -- Xếp loại tốt nghiệp
    gr.graduation_rank_name AS graduation_rating,
    CASE 

        -- Xuất sắc
        WHEN gr.graduation_rank_name ILIKE '%xuất sắc%' 
        OR gr.graduation_rank_name ~* 'gpa\s*[3]\.[8-9]' 
        THEN 'Xuất sắc'

        -- Giỏi
        WHEN gr.graduation_rank_name ILIKE '%giỏi%' 
        AND gr.graduation_rank_name NOT ILIKE '%khá%' 
        THEN 'Giỏi'

        -- Khá (bao gồm trung bình khá, khá - giỏi)
        WHEN gr.graduation_rank_name ILIKE '%khá%' 
        THEN 'Khá'

        -- Trung bình
        WHEN gr.graduation_rank_name ILIKE '%trung bình%' 
        THEN 'Trung bình'

        -- Chưa tốt
        WHEN gr.graduation_rank_name ILIKE '%chưa%' 
        THEN 'Chưa TN'

        -- Không rõ
        WHEN gr.graduation_rank_name ILIKE '%unknown%' 
        OR gr.graduation_rank_name ILIKE '%khác%' 
        THEN 'Unknown'

        ELSE gr.graduation_rank_name

        END as graduation_rating_group,
    
    -- Audit info
    ed.create_date AS record_created_at,
    ed.etl_datetime AS etl_datetime
FROM education ed
LEFT JOIN schools s ON ed.study_school_id = s.school_id
LEFT JOIN academic_levels al ON ed.academic_level_id = al.academic_level_id
LEFT JOIN qualifications q ON ed.qualification_id = q.qualification_id
LEFT JOIN grad_ranks gr ON ed.rank_id = gr.graduation_rank_id
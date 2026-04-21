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
        -- =========================
        -- 1. LANGUAGE (ưu tiên)
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(ngôn ngữ|tiếng nhật|tiếng anh|tiếng hàn|tiếng trung'
        '|japanese|日本語|korean|chinese|english'
        '|nhật bản học|hàn quốc học|biên.*phiên dịch|phương đông học'
        '|(it|cntt|công nghệ thông tin).*nhật)'
            THEN 'Language'

        -- =========================
        -- 2. TECH - SOFTWARE (MỞ RỘNG)
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(software|software engineer|kỹ sư phần mềm|kỹ thuật phần mềm|công nghệ phần mềm'
        '|computer science|computer science and engineering|khoa học máy tính'
        '|lập trình|programming|developer|dev\b'
        '|web|frontend|backend|fullstack|mobile'
        '|it-web)'
            THEN 'Tech - Software'

        -- =========================
        -- 3. TECH - DATA / INFRA / QA
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(data|ai|machine learning|deep learning|nlp|khoa học dữ liệu|phân tích dữ liệu'
        '|network|mạng|an ninh|security|devops|cloud'
        '|qa\b|qc\b|tester|kiểm thử)'
            THEN 'Tech - Data/Infra/QA'

        -- =========================
        -- 4. TECH - GENERAL IT (CỰC QUAN TRỌNG)
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(công nghệ thông tin|information technology|\bit\b|cntt)'
            THEN 'Tech - Software'

        -- =========================
        -- 5. BUSINESS / FINANCE
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(kinh tế|business|tài chính|finance|ngân hàng|banking|kế toán|accounting|kiểm toán)'
            THEN 'Business / Finance'

        -- =========================
        -- 6. MARKETING / DESIGN
        -- =========================
        WHEN LOWER(q.qualification_name) ~ 
        '(marketing|truyền thông|design|thiết kế|đồ họa|ui|ux)'
            THEN 'Marketing / Design'

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
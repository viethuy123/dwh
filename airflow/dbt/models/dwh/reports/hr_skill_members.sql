{{ config(materialized='table') }}

WITH all_employees AS (
    SELECT * FROM {{ ref('dim_odoo_members') }} -- Bảng danh mục nhân viên chuẩn
),

employee_skills AS (
    SELECT * FROM {{ ref('fct_member_skill') }} -- Bảng fact kết nối User - Skill - Level
),

skills_info AS (
    SELECT * FROM {{ ref('dim_odoo_skill') }} -- Bảng danh mục kỹ năng
)

SELECT
    -- 1. Thông tin nhân viên (Luôn có dữ liệu)
    e.member_id,
    e.member_name,
    e.member_code,
    e.position_name,
    e.branch_name,
    e.branch_code,
    e.division_name,
    e.division_group,
    e.member_level,
    e.member_status,
    e.member_status_detail,
    e.group_role_name as position_company_group,

    -- 2. Thông tin kỹ năng (Sẽ NULL nếu nhân viên chưa có skill)
    s.skill_id,
    s.skill_name,
    s.skill_type_name,
    s.is_language_type,
    s.level_group as skill_level_group,

    -- 3. Thông tin trình độ (Sẽ NULL nếu nhân viên chưa có skill)
    f.level_name,
    f.level_progress_percentage,
    f.updated_at AS last_assessed_at,

    -- 4. Logic hỗ trợ báo cáo
    CASE 
        WHEN s.skill_id IS NULL THEN 'No Skill Recorded'
        ELSE 'Has Skill'
    END AS skill_status,
    e.etl_datetime

FROM all_employees e
LEFT JOIN employee_skills f ON e.member_id = f.member_id
LEFT JOIN skills_info s ON f.skill_id = s.skill_id
-- models/marts/hr/fct_member_skill.sql
{{ config(materialized='table') }}

WITH member_skill AS (
    SELECT * FROM {{ ref('odoo_hr_member_skill') }}
),

skill_level AS (
    SELECT * FROM {{ ref('dim_skill_level') }}
)

SELECT
    es.member_skill_id,
    es.member_id,      -- Đây là ID để bạn join với bảng Employee/User
    es.skill_id,         -- Join với dim_skill
    es.skill_level_id,   -- Join với dim_skill_level
    
    -- Denormalize một vài trường quan trọng để BI query nhanh hơn
    sl.level_name,
    sl.level_group,
    sl.level_progress_percentage,
    
    -- Audit
    es.created_at,
    es.updated_at,
    es.etl_datetime
FROM member_skill es
LEFT JOIN skill_level sl ON es.skill_level_id = sl.skill_level_id
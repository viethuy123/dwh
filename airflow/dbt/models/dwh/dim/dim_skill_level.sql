-- models/marts/hr/dim_skill_level.sql
{{ config(materialized='table') }}

SELECT
    skill_level_id,
    skill_type_id,
    level_name,
    CASE 
        -- Nhóm theo số thứ tự (Dành cho các loại Mức 1, 2, 3... hoặc 1. Beginner)
        WHEN level_name ~* '1|Beginner|Fresher|A1|N5|Topik 1|HSK 1' THEN 'Beginner / Level 1'
        WHEN level_name ~* '2|Elementary|Junior|A2|N4|Topik 2|HSK 2' THEN 'Elementary / Level 2'
        WHEN level_name ~* '3|Intermediate|Middle|B1|N3|Topik 3|HSK 3' THEN 'Intermediate / Level 3'
        WHEN level_name ~* '4|Senior|Advanced|B2|N2|Topik 4|HSK 4|Upper' THEN 'Advanced / Level 4'
        WHEN level_name ~* '5|6|Expert|Master|Native|C1|C2|N1|Topik 5|Topik 6|HSK 5|HSK 6' THEN 'Expert / High Level'
        
        -- Case mới phát sinh sẽ rơi vào đây
        ELSE level_name
    END AS level_group,
    level_progress_percentage,
    is_default_level,
    is_selectable,
    etl_datetime
FROM {{ ref('odoo_hr_skill_level') }}
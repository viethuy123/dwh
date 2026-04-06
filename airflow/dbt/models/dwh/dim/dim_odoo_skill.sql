-- models/marts/hr/dim_skill.sql
{{ config(materialized='table') }}

WITH skill AS (
    SELECT * FROM {{ ref('odoo_hr_skill') }}
),

skill_type AS (
    SELECT * FROM {{ ref('odoo_hr_skill_type') }}
)

SELECT
    s.skill_id,
    s.skill_name,
    st.skill_type_id,
    st.skill_type_name,
    st.is_language_type,
    s.is_selectable AS is_active,
    s.sequence_order,
    s.etl_datetime
FROM skill s
LEFT JOIN skill_type st ON s.skill_type_id = st.skill_type_id
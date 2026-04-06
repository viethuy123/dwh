-- models/marts/hr/dim_skill_level.sql
{{ config(materialized='table') }}

SELECT
    skill_level_id,
    skill_type_id,
    level_name,
    level_progress_percentage,
    is_default_level,
    is_selectable,
    etl_datetime
FROM {{ ref('odoo_hr_skill_level') }}
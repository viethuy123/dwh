{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_skill_level') }}
),

transformed AS (
    SELECT
        -- Primary & Foreign Keys
        CAST(id AS INTEGER) AS skill_level_id,
        CAST(skill_type_id AS INTEGER) AS skill_type_id,
        
        -- Core Attributes
        CAST(name AS VARCHAR) AS level_name,
        CAST(level_progress AS INTEGER) AS level_progress_percentage,
        
        -- Flags (Booleans)
        CAST(default_level AS BOOLEAN) AS is_default_level,
        CAST(allow_select AS BOOLEAN) AS is_selectable,
        
        -- Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM transformed
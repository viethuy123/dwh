{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_employee_skill_log') }}
),

transformed AS (
    SELECT
        -- Primary Key
        CAST(id AS INTEGER) AS skill_log_id,
        
        -- Foreign Keys
        CAST(employee_id AS INTEGER) AS employee_id,
        CAST(department_id AS INTEGER) AS department_id,
        CAST(skill_id AS INTEGER) AS skill_id,
        CAST(skill_level_id AS INTEGER) AS skill_level_id,
        CAST(skill_type_id AS INTEGER) AS skill_type_id,
        
        -- Metrics & Dates
        CAST(level_progress AS INTEGER) AS level_progress,
        CAST(date AS DATE) AS log_date,
        
        -- Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM transformed
{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_z_academic_level') }}
),

transformed AS (
    SELECT
        -- Primary & Foreign Keys
        CAST(id AS INTEGER) AS academic_level_id,
        CAST(company_id AS INTEGER) AS company_id,
        
        -- Academic Details
        CAST(name AS VARCHAR) AS level_name,
        CAST(certificate AS VARCHAR) AS certificate_name,
        CAST(specialized AS VARCHAR) AS specialized_field,
        CAST(school AS VARCHAR) AS school_name,
        CAST(training_system AS VARCHAR) AS training_system,
        CAST(note AS TEXT) AS note,
        CAST(sequence AS INTEGER) AS sequence_order,
        
        -- Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM transformed
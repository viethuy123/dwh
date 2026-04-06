{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_skill_type') }}
),

transformed AS (
    SELECT
        -- Primary Key
        CAST(id AS INTEGER) AS skill_type_id,
        
        -- Core Attributes
        COALESCE(
            {{ parse_python_json('name') }}->>'vi_VN',
            {{ parse_python_json('name') }}->>'en_US',
            'unknown'
        ) AS skill_type_name,
        CAST(sequence AS INTEGER) AS sequence_order,
        CAST(color AS INTEGER) AS color_index,
        
        -- Flags
        CAST(active AS BOOLEAN) AS is_active,
        CAST(is_language AS BOOLEAN) AS is_language_type,
        
        -- Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM transformed
{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_z_qualification') }}
),

transformed AS (
    SELECT
        -- Primary & Foreign Keys
        CAST(id AS INTEGER) AS qualification_id,
        CAST(company_id AS INTEGER) AS company_id,
        
        -- Qualification Details
        CAST(name AS VARCHAR) AS qualification_name,
        CAST(note AS TEXT) AS note,
        
        -- Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM transformed
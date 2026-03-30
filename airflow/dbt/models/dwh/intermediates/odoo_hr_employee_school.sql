{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_employee_school') }}
),

renamed AS (
    SELECT
        -- Primary Key
        CAST(id AS BIGINT) AS school_id,

        -- Location & Foreign Keys
        CAST(state_id AS TEXT) AS state_id, -- Trong Odoo đôi khi state là text code (VD: 'VN-HN')
        CAST(country_id AS DOUBLE PRECISION) AS country_id,

        -- School Info
        CAST(name AS TEXT) AS school_name,
        CAST(type AS TEXT) AS school_type,
        CAST(note AS TEXT) AS note,

        -- Audit Columns
        CAST(create_uid AS BIGINT) AS create_uid,
        CAST(write_uid AS BIGINT) AS write_uid,
        CAST(create_date AS TIMESTAMP) AS create_date,
        CAST(write_date AS TIMESTAMP) AS write_date,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime

    FROM source
)

SELECT * FROM renamed
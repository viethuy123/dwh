{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_graduation_rank') }}
),

renamed AS (
    SELECT
        -- Primary Key
        CAST(id AS BIGINT) AS graduation_rank_id,

        -- Info
        CAST(name AS TEXT) AS graduation_rank_name,

        -- Audit Columns
        CAST(create_uid AS BIGINT) AS create_uid,
        CAST(write_uid AS BIGINT) AS write_uid,
        CAST(create_date AS TIMESTAMP) AS create_date,
        CAST(write_date AS TIMESTAMP) AS write_date,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime

    FROM source
)

SELECT * FROM renamed
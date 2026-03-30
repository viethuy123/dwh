{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        CAST(id AS INTEGER) AS id,
        CAST(sequence AS INTEGER) AS sequence,
        CAST(evaluation_group_job AS INTEGER) AS evaluation_group_job,
        CAST(create_uid AS INTEGER) AS create_uid,
        CAST(write_uid AS INTEGER) AS write_uid,
        CAST(name AS VARCHAR) AS name,
        CAST(create_date AS TIMESTAMP) AS create_date,
        CAST(write_date AS TIMESTAMP) AS write_date,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
    FROM {{ source('odoo', 'stg_odoo_hr_rank') }} -- Thay 'raw_data' bằng tên source của bạn
)

SELECT *
FROM source_data
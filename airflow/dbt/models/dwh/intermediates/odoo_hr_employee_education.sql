{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_employee_education') }}
),

renamed AS (
    SELECT
        -- IDs Chính
        CAST(id AS BIGINT) AS education_id,
        CAST(employee_id AS DOUBLE PRECISION) AS employee_id,
        CAST(applicant_id AS DOUBLE PRECISION) AS applicant_id,

        -- Khóa ngoại (Reference IDs)
        CAST(study_school AS DOUBLE PRECISION) AS study_school_id,
        CAST(academic_level_id AS DOUBLE PRECISION) AS academic_level_id,
        CAST(qualification_id AS DOUBLE PRECISION) AS qualification_id,
        CAST(rank_id AS DOUBLE PRECISION) AS rank_id,

        -- Thông tin chi tiết (Text)
        CAST(graduation_year AS TEXT) AS graduation_year,
        CAST(faculty AS TEXT) AS faculty,

        -- Thông tin hệ thống (Audit columns)
        CAST(create_uid AS BIGINT) AS create_uid,
        CAST(write_uid AS BIGINT) AS write_uid,
        CAST(create_date AS TIMESTAMP) AS create_date,
        CAST(write_date AS TIMESTAMP) AS write_date,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime

    FROM source
)

SELECT * FROM renamed
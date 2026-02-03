{{ config(materialized='table') }}

SELECT

    staff_code,
    skill_name,
    etl_datetime
FROM {{ source('excel', 'stg_skill_members') }}
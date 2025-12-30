{{ config(materialized='table') }}

SELECT

    staff_code,
    skill_name
FROM {{ source('excel', 'stg_skill_members') }}
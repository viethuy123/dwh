{{ config(materialized='table') }}

SELECT

    *
FROM {{ source('excel', 'stg_skill_members') }}
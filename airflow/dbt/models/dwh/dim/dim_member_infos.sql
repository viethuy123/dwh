{{ config(materialized='table') }}


SELECT 
    *
FROM {{ ref('user_infos') }}
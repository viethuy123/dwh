{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name 
FROM {{ ref('branches') }}
{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name 
FROM {{ source('dwh', 'branches') }}
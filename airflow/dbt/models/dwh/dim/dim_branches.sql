{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name,
    etl_datetime
FROM {{ ref('branches') }}
{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name,
    branch_code,
    branch_address,
    etl_datetime
FROM {{ ref('branches') }}
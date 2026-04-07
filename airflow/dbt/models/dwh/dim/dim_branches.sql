{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name,
    coalesce(branch_code, 'TOKYO') as branch_code,
    branch_address,
    etl_datetime
FROM {{ ref('branches') }}
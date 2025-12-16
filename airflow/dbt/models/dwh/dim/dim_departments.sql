{{ config(materialized='table') }}


SELECT 
    department_id, 
    department_name 
FROM {{ source('dwh', 'departments') }}
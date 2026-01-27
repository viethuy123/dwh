{{ config(materialized='table') }}


SELECT 
    department_id, 
    department_name 
FROM {{ ref('departments') }}
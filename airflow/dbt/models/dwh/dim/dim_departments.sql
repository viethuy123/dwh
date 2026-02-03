{{ config(materialized='table') }}


SELECT 
    department_id, 
    department_name ,
    etl_datetime
FROM {{ ref('departments') }}
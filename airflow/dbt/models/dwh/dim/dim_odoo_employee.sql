{{ config(materialized='table') }}


SELECT 
* 
FROM {{ ref('odoo_hr_employee') }}

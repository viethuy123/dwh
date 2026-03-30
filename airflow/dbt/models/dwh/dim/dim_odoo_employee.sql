{{ config(materialized='table') }}

WITH education AS (
    SELECT 
    * 
    FROM {{ ref('odoo_hr_employee') }}
)
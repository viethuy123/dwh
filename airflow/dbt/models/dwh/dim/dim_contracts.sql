{{ config(materialized='table', enabled=false) }}

select * from {{ ref('odoo_hr_contract') }}
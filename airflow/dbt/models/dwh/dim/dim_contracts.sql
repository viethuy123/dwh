{{ config(materialized='table') }}

select * from {{ ref('odoo_hr_contract') }}
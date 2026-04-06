
{{ config(materialized='table') }}

select * from {{ ref('odoo_branch') }}
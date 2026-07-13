{{ config(materialized='table') }}

SELECT 
    type_member_id , 
    member_status_detail
FROM {{ ref('dim_odoo_members') }}
group by 1,2
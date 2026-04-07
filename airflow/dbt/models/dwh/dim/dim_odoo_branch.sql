
{{ config(materialized='table') }}

select * ,
    case
        when id = 11 then 'Chi nhánh Đà Nẵng'
        when id = 10 then 'GMO_Customer'
        else branch_name 
    end as branch_group_name,
    case
        when id = 11 then 'ĐN'
        when id = 10 then 'CUS'
        else coalesce(branch_code, 'TOKYO') 
    end as branch_group_code,

from {{ ref('odoo_branch') }}
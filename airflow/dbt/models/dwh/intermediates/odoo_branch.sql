{{ config(materialized='table') }}

select 
    id,
    name as branch_name,
    x_branch_office as address,
    coalesce(branch_code, 'TOKYO') as branch_code,


    -- CAST(created_at AS TIMESTAMP) as created_at,
    etl_datetime



from {{ source('odoo', 'stg_odoo_res_company') }}
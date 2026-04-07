{{ config(materialized='table') }}


SELECT 
    branch_id, 
    branch_name,
    case
        when branch_id = 11 then 'Chi nhánh Đà Nẵng'
        when branch_id = 10 then 'GMO_Customer'
        else branch_name 
    end as branch_group_name,
    coalesce(branch_code, 'TOKYO') as branch_code,
    case
        when branch_id = 11 then 'ĐN'
        when branch_id = 10 then 'CUS'
        else coalesce(branch_code, 'TOKYO') 
    end as branch_group_code,
    
    branch_address,
    etl_datetime
FROM {{ ref('branches') }}
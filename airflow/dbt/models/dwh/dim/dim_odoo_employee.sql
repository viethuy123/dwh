{{ config(materialized='table') }}

with contracts as (
    select 
        employee_code,
        row_number() over (partition by z_employee_code order by date_start desc) as rn,
        contract_type
    from {{ ref('odoo_hr_contract') }}
),
latest_contracts as (
    select 
        employee_code,
        contract_type
    from contracts
    where rn = 1
)
SELECT 
e.* ,
lc.contract_type
FROM {{ ref('odoo_hr_employee') }} e
LEFT JOIN latest_contracts lc
    ON cast(e.z_employee_code AS INTEGER) = cast(lc.z_employee_code AS INTEGER)

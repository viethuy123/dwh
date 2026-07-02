{{ config(materialized='table') }}
with 
divisions as (
select 
    d.id, 
    d.company_id as branch_id,
    d.manager_id,
    d1.complete_name as complete_parent_name,
    d.complete_name as complete_name,
    COALESCE(
            {{ parse_python_json('d.name') }}->>'vi_VN',
            {{ parse_python_json('d.name') }}->>'en_US',
            'unknown'
        ) AS division_name,
    COALESCE(
            {{ parse_python_json('d1.name') }}->>'vi_VN',
            {{ parse_python_json('d1.name') }}->>'en_US',
            'unknown'
        ) AS division_parent_name,
    COALESCE(
            {{ parse_python_json('d2.name') }}->>'vi_VN',
            {{ parse_python_json('d2.name') }}->>'en_US',
            'unknown'
        ) AS division_master_name,
    
    d.active as status,
    d.etl_datetime
from {{ source('odoo','stg_odoo_hr_department') }} d
left join {{ source('odoo','stg_odoo_hr_department') }} d1 
on d.parent_id = d1.id
left join {{ source('odoo','stg_odoo_hr_department') }} d2
on d.master_department_id = d2.id
),


division_group as (
select 
    id,
    branch_id,
    manager_id,
    complete_parent_name,
    complete_name,
    division_name,
    COALESCE(division_master_name, division_name) AS division_group,
    division_parent_name,
    status,
    etl_datetime    
from divisions
)
select * from division_group
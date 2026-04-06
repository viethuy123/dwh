{{ config(materialized='table') }}


select 
    hj.job_id,
    hj.department_id as division_id,
    hj.company_id as branch_id,
    hj.manager_id,
    hj.group_role_id,
    hj.group_job_id,
    hj.job_code,
    coalesce(
        {{ parse_python_json('hj.name_json') }}->>'vi_VN',
        {{ parse_python_json('hj.name_json') }}->>'en_US',
        'unknown'
    ) as job_name,
    coalesce(
        {{ parse_python_json('hj1.name_json') }}->>'vi_VN',
        {{ parse_python_json('hj1.name_json') }}->>'en_US',
        'unknown'
    ) as group_role_name,
    hj.etl_datetime


from {{ ref('odoo_hr_job') }} hj
left join {{ ref('odoo_hr_job') }} hj1
    on hj.group_role_id = hj1.job_id
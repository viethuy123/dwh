{{ config(materialized='table') }}

with 
-- lấy hợp đồng mới nhất của mỗi nhân viên để biết được loại hợp đồng hiện tại của họ
contracts as (
    select 
        member_code,
        row_number() over (partition by member_code order by date_start desc) as rn,
        contract_type
    from {{ ref('odoo_hr_contract') }}
),
latest_contracts as (
    select 
        member_code,
        contract_type
    from contracts
    where rn = 1
)




SELECT 
    e.member_id,
    e.name as member_name,
    e.work_email as member_email,
    e.member_code as member_code,
    e.member_type,
    e.gender,
    e.marital,
    e.job_id,
    initcap(
        lower(
            COALESCE(e.level, 'FRESHER')
            )
    ) as member_level,
    b.branch_name,
    b.branch_code,
    d.division_name,
    d.division_group,
    COALESCE(e.job_title, 'Unknown') AS position_name,
    COALESCE(j.group_role_name, 'Unknown') AS group_role_name,
    case 
        when e.is_active then 'Active' 
        else 'Resigned' end 
    as member_status,
    lc.contract_type,
    COALESCE(
            EXTRACT(YEAR FROM e.joining_date)
            - EXTRACT(YEAR FROM e.birthday),
            0
    )::INT                                              
    AS age_at_hire,

    -- học vấn
    e.rank_id,
    e.academic_level_id,
    e.qualification_id,

    -- date
    e.birthday,
    e.joining_date,
    e.start_working_date as official_date,
    e.probation_start_date as probation_date,
    e.traineeship_start_date as traineeship_date,
    e.departure_date as departure_date,
    e.resign_date as resign_date,
    coalesce(e.resign_date, e.departure_date) as end_date,
    e.etl_datetime


FROM {{ ref('odoo_hr_member') }} e
LEFT JOIN latest_contracts lc
    ON cast(e.member_code AS INTEGER) = cast(lc.member_code AS INTEGER)
left join {{ ref('dim_odoo_branch') }} b
    on e.company_id = b.id
left join {{ ref('dim_odoo_division') }} d
    on e.division_id = d.id
left join {{ ref('dim_odoo_job') }} j
    on e.job_id = j.job_id


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
    b.branch_name as branch_root_name,
    b.branch_code as branch_root_code,
    b.branch_group_name as branch_name,
    b.branch_group_code as branch_code,
    d.division_name,
    d.division_group,
    COALESCE(e.job_title, 'Unknown') AS position_name,
    CASE 
        -- INTERN / CTV
        WHEN e.job_title ILIKE '%intern%' 
        OR e.job_title ILIKE '%thử việc%' 
        OR e.job_title ILIKE '%học việc%' 
        OR e.job_title ILIKE '%cộng tác viên%' 
        OR e.job_title ILIKE '%CTV%' 
        THEN 'INTERN_CTV'

        -- MANAGEMENT
        WHEN e.job_title ILIKE '%manager%' 
        OR e.job_title ILIKE '%director%' 
        OR e.job_title ILIKE '%leader%' 
        OR e.job_title ILIKE '%head%' 
        OR e.job_title ILIKE '%ceo%' 
        OR e.job_title ILIKE '%cto%' 
        THEN 'MANAGEMENT'

        -- ENGINEERING
        WHEN e.job_title ILIKE '%developer%' 
        OR e.job_title ILIKE '%engineer%' 
        OR e.job_title ILIKE '%ai%' 
        OR e.job_title ILIKE '%data%' 
        OR e.job_title ILIKE '%tester%' 
        OR e.job_title ILIKE '%qa%' 
        OR e.job_title ILIKE '%devops%' 
        OR e.job_title ILIKE '%cloud%' 
        THEN 'ENGINEERING'

        -- PRODUCT / BA
        WHEN e.job_title ILIKE '%ba%' 
        OR e.job_title ILIKE '%business analyst%' 
        OR e.job_title ILIKE '%product%' 
        THEN 'PRODUCT_BA'

        -- SALES
        WHEN e.job_title ILIKE '%sale%' 
        OR e.job_title ILIKE '%account%' 
        OR e.job_title ILIKE '%business development%' 
        THEN 'SALES'

        -- MARKETING
        WHEN e.job_title ILIKE '%marketing%' 
        OR e.job_title ILIKE '%mkt%' 
        OR e.job_title ILIKE '%content%' 
        THEN 'MARKETING'

        -- HR / ADMIN
        WHEN e.job_title ILIKE '%hr%' 
        OR e.job_title ILIKE '%admin%' 
        OR e.job_title ILIKE '%accountant%' 
        OR e.job_title ILIKE '%legal%' 
        THEN 'HR_ADMIN'

        -- OPERATION
        WHEN e.job_title ILIKE '%project%' 
        OR e.job_title ILIKE '%delivery%' 
        OR e.job_title ILIKE '%support%' 
        OR e.job_title ILIKE '%operation%' 
        THEN 'OPERATION'

        ELSE 'OTHER'

        END as position_group,
    INITCAP(
        LOWER(
            CASE 
                WHEN j.group_role_name IS NULL OR LOWER(j.group_role_name) = 'unknown' THEN 'Khác'
                ELSE j.group_role_name 
            END
        )
    ) AS group_role_name,
    -- COALESCE(j.group_role_name, 'Unknown') AS group_role_name,
    e.state as member_status,
    e.state_root as member_status_root,
    lc.contract_type,
    initcap(lower(
        CASE 
            -- WHEN b.branch_group_code = 'Onsite' THEN 'Onsite'
             -- Trả về giá trị cột, không để trong nháy đơn
            WHEN e.start_working_date is not NULL THEN 'Official' 
            WHEN e.probation_start_date is not NULL THEN 'Probationary' 
            WHEN e.traineeship_start_date is not NULL THEN 'Apprentice'
            WHEN lc.contract_type IS NOT NULL THEN lc.contract_type
            ELSE 'Unknown'
        END
    )) AS member_status_detail,
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
    e.issue_date_identification,
    e.birthday,
    e.joining_date,
    e.start_working_date,
    coalesce(e.start_working_date,e.probation_start_date,e.traineeship_start_date, e.joining_date, e.departure_date ,e.resign_date) as official_date,
    e.probation_start_date as probation_date,
    e.traineeship_start_date as traineeship_date,
    e.departure_date as departure_date,
    e.resign_date as resign_date,
    coalesce(e.resign_date) as end_date,
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

where e.member_code > 1000


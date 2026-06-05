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
),
member_ranking as (
    select 
        *,
        row_number() over (
            partition by member_code 
            order by 
                is_active desc, 
                member_id desc
        ) as member_rn
    from {{ ref('odoo_hr_member') }}
    where member_code > 1000
),

filtered_members as (
    select *
    from member_ranking
    where member_rn = 1
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
    NULL AS position_group,
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
    t.member_type_name as member_status_detail_root,
    initcap(lower(
        CASE 
            WHEN t.member_type_name is not NULL then t.member_type_name
            WHEN e.start_working_date is not NULL THEN 'Chính thức' 
            WHEN e.probation_start_date is not NULL THEN 'Thử việc' 
            WHEN e.traineeship_start_date is not NULL THEN 'Thực tập'
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
    -- e.rank_id,
    -- e.academic_level_id,
    -- e.qualification_id,

    -- date
    e.issue_date_identification,
    e.birthday,
    e.joining_date,
    e.start_working_date,
    coalesce(
        e.traineeship_start_date, 
        e.probation_start_date,
        e.start_working_date,
        e.joining_date, 
        e.departure_date ,
        e.resign_date,
        CURRENT_DATE
    ) as official_date,
    e.probation_start_date as probation_date,
    e.traineeship_start_date as traineeship_date,
    e.departure_date as departure_date,
    e.resign_date as resign_date,
    coalesce(e.resign_date) as end_date,
    e.etl_datetime


FROM filtered_members e
left join {{ ref('odoo_z_type_employee') }} t
    on cast(e.type_member_id AS INTEGER) = cast(t.member_type_id AS INTEGER)
LEFT JOIN latest_contracts lc
    ON cast(e.member_code AS INTEGER) = cast(lc.member_code AS INTEGER)
left join {{ ref('dim_odoo_branch') }} b
    on e.company_id = b.id
left join {{ ref('dim_odoo_division') }} d
    on e.division_id = d.id
left join {{ ref('dim_odoo_job') }} j
    on e.job_id = j.job_id



{{ config(materialized='table') }}

with _total_bill as(
    select 
    "projectObjId" as project_id,
    sum(coalesce("billEffortMenMonth",0)) as total_project_bill_cost
    from {{ source('create', 'stg_create_project_bill_costs') }}
    group by "projectObjId"
),

pod as (
    select 
    *,
    substring(jira_url FROM '(?:\/browse\/|\/projects\/|\/project-config\/)([A-Z0-9]+)') AS code_jira

    from {{ ref('pods') }}
),
pod_project as (
    select 
    *,
    upper(coalesce(code_jira, project_code)) as code_join

    from pod
),


create_project as (
    select * ,
    CASE 
        WHEN LEAD(cp.created_time, 1 , TIMESTAMP '2999-12-31') 
        OVER(PARTITION BY cp.project_code ORDER BY cp.created_time ASC) = TIMESTAMP '2999-12-31'
            THEN DATE '2999-12-31'
            ELSE (LEAD(cp.created_time, 1 , TIMESTAMP '2999-12-31') 
            OVER(PARTITION BY cp.project_code ORDER BY cp.created_time ASC) - INTERVAL '1 day')::DATE
        END AS end_date_1
    from {{ ref('create_project') }} as cp
),

jisseki_project as (
    select * from {{ ref('jisseki_project') }}
),

create_pod as (
SELECT 
    cp.id as create_pr_id,
    pp.id as pod_pr_id,
    pp.project_code as pod_project_code,
    coalesce(cp.project_name, pp.project_name) as project_name,
    coalesce(cp.project_code, pp.code_join) as project_code,
    coalesce(cp.project_type, pp.project_type) as project_type,
    pp.project_size,
    coalesce(cp.project_description, pp.project_overview) as project_description,
    coalesce(cp.project_rank, pp.project_rank) as project_rank,
    coalesce(cp.project_lead_id, pp.pm_id) as project_lead_id,
    coalesce(cp.project_div::TEXT, pp.department_id) as department_id,
    cp.note,
    cp.project_scope,
    pp.domain,
    pp.market_id,
    pp.sub_pm_id,
    pp.sale_id,
    (coalesce(cp.end_time, pp.plan_release_date)) as end_time,
    (coalesce(cp.start_time, pp.start_date)) as start_time,
    (coalesce(cp.created_time, pp.created_time)) as created_time
from create_project cp
full join pod_project pp
on cp.project_code = pp.code_join
),

all_project as (
    select 
        cp.create_pr_id,
        cp.pod_pr_id,
        cp.pod_project_code,
        jp.id as jisseki_pr_id,
        coalesce(cp.project_name, jp.project_name) as project_name,
        coalesce(cp.project_code, jp.project_code) as project_code,
        coalesce(cp.project_type, jp.project_type) as project_type,
        coalesce(cp.project_description, jp.summary) as project_description,
        coalesce(cp.project_rank, jp.project_rank) as project_rank,
        coalesce(cp.project_lead_id, jp.name_pm) as project_lead_id,
        cp.department_id,
        jp.project_branch,
        cp.note,
        coalesce(cp.project_scope, jp.project_scope) as project_scope,
        cp.domain,
        cp.market_id,
        cp.sub_pm_id,
        cp.sale_id,
        date(coalesce(cp.end_time, jp.end_time)) as end_date,
        date(coalesce(cp.start_time, jp.start_time)) as start_date,
        date(coalesce(cp.created_time, jp.created_time)) as created_date

    from create_pod cp
    full join jisseki_project jp
    on cp.project_code = jp.project_code
)
select * from all_project
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21



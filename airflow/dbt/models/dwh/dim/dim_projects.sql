{{ config(materialized='table') }}


WITH project_values AS (
    SELECT 
        project_id,
        sum(coalesce(value,0) + coalesce(value1,0)) as project_value
    FROM {{ ref('project_profit_loss') }}
    GROUP BY project_id
),
-- SELECT 
--     a.*s
-- FROM {{ ref('projects') }} a

create_jira_project as (
    select 
    cp.id::TEXT,
    case 
    when jp.id is not null then 'jira' else 'create_jira' end as project_source,
    cp.project_code,
    cp.project_name,
    cp.project_status,
    project_description,
    project_lead_id,
    cp.project_type,
    note,
    status,
    type,
    project_objective,
    project_rank,
    project_scope,

    null as name_pm,
    null as name_br_se,
    null as project_branch,
    null as point_comment,
    null as team_size,

    null as project_size,
    null as project_category_pod,
    null as department_id,
    null as pod_status,
    null as is_deleted,
    null as sale_id,
    null as domain,
    null as market_id,
    null as sub_pm_id,

    cp.start_time,
    cp.end_time
    
    from {{ ref('create_project') }} cp
    left join {{ ref('jira_project') }} jp
    on cp.project_code = jp.project_key
    and cp.jira_project_id = jp.id
),
jisseki_project as (
    select 
    id::TEXT,
    'jisseki' as project_source,
    project_code,
    project_name,
    status::TEXT as project_status,
    summary as project_description,
    null as project_lead_id,
    project_type,
    null as note,
    null as status,
    null as type,
    null as project_objective,
    project_rank,
    project_scope,

    name_pm,
    name_br_se,
    project_branch,
    point_comment,
    team_size,

    null as project_size,
    null as project_category_pod,
    null as department_id,
    null as pod_status,
    null as is_deleted,
    null as sale_id,
    null as domain,
    null as market_id,
    null as sub_pm_id,

    start_time,
    end_time
    
    from {{ ref('jisseki_project') }}
),
pod_project as (
    select
    pod_id::TEXT as id,
    'pod' as project_source,
    project_code,
    project_name,
    status as project_status,
    project_overview as project_description,
    pm_id as project_lead_id,
    project_type,
    null as note,
    null as status,
    null as type,
    null as project_objective,
    project_rank,
    null as project_scope,

    null as name_pm,
    null as name_br_se,
    null as project_branch,
    null as point_comment,
    null as team_size,

    project_size,
    project_category as project_category_pod,
    department_id,
    pod_status,
    is_deleted,
    sale_id,
    domain,
    market_id,
    sub_pm_id,

    start_date as start_time,
    null::TIMESTAMP WITH TIME ZONE as end_time
    from {{ ref('pods') }}
)

select * from create_jira_project
union
select * from jisseki_project
union
select * from pod_project

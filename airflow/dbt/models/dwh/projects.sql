{{ config(materialized='table') }}

with _total_bill as(
    select 
    "projectObjId" as project_id,
    sum(coalesce("billEffortMenMonth",0)) as total_project_bill_cost
    from {{ source('create', 'stg_project_bill_costs') }}
    group by "projectObjId"
)
SELECT 
    a._id as project_id,
    a."jiraProjectId" as jira_project_id,
    a."projectCode" as project_code,
    a."projectName" as project_name,
    a."projectJiraUrl" as project_jira_url,
    a."projectDescription" as project_description,
    b."projectCategoryName" as project_category,
    a."projectStatus" as project_status,
    a."projectType" as project_type,
    d.total_project_bill_cost as project_bill,
    c.name_pm as name_pm,
    c.name_br_se as name_brse,
    c.location as location,
    c.scope as scope,
    a.type as type,
    c.point_css as point_css,
    c.point_comment as point_comment,
    c.summary as summary,
    c.size as size,
    c.period as period,
    a."projectRank" as project_rank,
    c.team_size as team_size,
    c."startDate" as start_date,
    c."endDate" as end_date,
    a."isDeleted" as is_deleted,
    c.updated_at as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_projects') }} a
LEFT JOIN {{ source('create', 'stg_project_categories') }} b
ON a."projectCategoryObjId" = b._id
LEFT JOIN {{ source('jisseki', 'stg_projects') }} c
ON a."projectCode" = c.code
LEFT JOIN _total_bill d
ON a._id = d.project_id
where a."jiraType" = 'JIRA8'